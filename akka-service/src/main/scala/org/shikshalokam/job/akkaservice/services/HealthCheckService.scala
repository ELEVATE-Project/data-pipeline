package org.shikshalokam.job.akkaservice.services

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.shikshalokam.job.akkaservice.functions.Functions
import org.shikshalokam.job.akkaservice.models.JsonProtocol._
import org.shikshalokam.job.akkaservice.models._
import spray.json._

import java.time.Instant
import java.util.{Collections, Properties, UUID}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object HealthCheckService {

  implicit val system: ActorSystem = ActorSystem("healthcheck-service-core")
  implicit val mat: Materializer = Materializer(system)
  implicit val ec: ExecutionContext = system.dispatcher

  private val config = Functions.load()
  private val http = Http()

  def checkFlink(): Future[FlinkHealth] = {
    val base = config.flink.restApiUrl
    val unhealthyCluster = FlinkClusterHealth("UNHEALTHY", 0, 0, 0)

    val overviewFlink = http.singleRequest(HttpRequest(uri = s"$base/overview"))
      .flatMap(_.entity.toStrict(5.seconds))
      .map(_.data.utf8String.parseJson)
      .map { json =>
        val obj = json.asJsObject
        val tm = obj.fields("taskmanagers").convertTo[Int]
        val st = obj.fields("slots-total").convertTo[Int]
        val sa = obj.fields("slots-available").convertTo[Int]
        FlinkClusterHealth(
          status = if (tm > 0 && sa >= 0) "HEALTHY" else "UNHEALTHY",
          taskmanagers = tm,
          slotsTotal = st,
          slotsAvailable = sa
        )
      }
      .recover { case _ => unhealthyCluster }

    val jobsFlink = http.singleRequest(HttpRequest(uri = s"$base/jobs/overview"))
      .flatMap(_.entity.toStrict(5.seconds))
      .map(_.data.utf8String.parseJson)
      .flatMap { json =>
        val jobArray = json.asJsObject.fields("jobs").convertTo[List[JsObject]]

        // Check all configured job names
        Future.sequence(config.flink.jobs.map { jobName =>
          jobArray.find(_.fields("name").convertTo[String] == jobName) match {
            case None =>
              Future.successful(FlinkJobStatus(jobName, "NOT_FOUND"))

            case Some(jobObj) =>
              val jid = jobObj.fields("jid").convertTo[String]

              http.singleRequest(HttpRequest(uri = s"$base/jobs/$jid"))
                .flatMap(_.entity.toStrict(5.seconds))
                .map(_.data.utf8String.parseJson)
                .map { details =>
                  val state = details.asJsObject.fields("state").convertTo[String]
                  FlinkJobStatus(jobName, state)
                }
          }
        })
      }
      .recover { case _ => config.flink.jobs.map(j => FlinkJobStatus(j, "UNREACHABLE")) }

    for {
      overview <- overviewFlink
      jobs <- jobsFlink
    } yield FlinkHealth(overview, jobs)
  }

  def checkMetabase(): Future[MetabaseHealth] = {
    val url = s"${config.metabase.url}/api/health"

    http.singleRequest(HttpRequest(uri = url))
      .flatMap { resp =>
        resp.entity.discardBytes()
        Future.successful(MetabaseHealth(
          if (resp.status.isSuccess()) "HEALTHY" else "UNHEALTHY",
          config.metabase.url
        ))
      }
      .recover { case _ => MetabaseHealth("UNHEALTHY", config.metabase.url) }
  }

  def checkKafka(): Future[KafkaHealth] = Future {
    val topic = "_healthcheck_test"
    val uuid = UUID.randomUUID().toString
    var producer: KafkaProducer[String, String] = null
    var consumer: KafkaConsumer[String, String] = null

    try {
      val producerProps = new Properties()
      producerProps.put("bootstrap.servers", config.kafka.broker)
      producerProps.put("key.serializer", classOf[StringSerializer].getName)
      producerProps.put("value.serializer", classOf[StringSerializer].getName)

      producer = new KafkaProducer[String, String](producerProps)
      producer.send(new ProducerRecord(topic, uuid))
      producer.flush()

      val consumerProps = new Properties()
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.broker)
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "hc-" + UUID.randomUUID())
      consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

      consumer = new KafkaConsumer[String, String](consumerProps)
      consumer.subscribe(Collections.singletonList(topic))
      consumer.poll(java.time.Duration.ofMillis(100))

      var found = false
      val deadline = System.currentTimeMillis() + 20000
      while (!found && System.currentTimeMillis() < deadline) {
        val records = consumer.poll(java.time.Duration.ofMillis(500))
        val it = records.iterator()
        while (it.hasNext) {
          if (it.next().value() == uuid) {
            found = true
          }
        }
      }
      KafkaHealth(if (found) "HEALTHY" else "UNHEALTHY", config.kafka.broker)
    } catch {
      case _: Exception => KafkaHealth("UNHEALTHY", config.kafka.broker)
    } finally {
      if (producer != null) producer.close()
      if (consumer != null) consumer.close()
    }
  }

  def fullHealth(): Future[FullHealthResponse] = {
    val flinkF = checkFlink()
    val kafkaF = checkKafka()
    val metaF = checkMetabase()

    for {
      flink <- flinkF
      kafka <- kafkaF
      meta <- metaF
    } yield FullHealthResponse(
      flink, kafka, meta, Instant.now.toString
    )
  }
}

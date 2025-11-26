package org.shikshalokam.job.healthcheckservice

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import spray.json._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import java.util.{Collections, Properties, UUID}
import java.time.Instant
import JsonProtocol._

case class FlinkJobStatus(name: String, status: String)
case class FlinkClusterHealth(status: String, taskmanagers: Int, slotsTotal: Int, slotsAvailable: Int)
case class FlinkHealth(cluster: FlinkClusterHealth, jobs: List[FlinkJobStatus])

case class KafkaHealth(status: String, broker: String)
case class MetabaseHealth(status: String, url: String)

case class FullHealthResponse(
                               flink: FlinkHealth,
                               kafka: KafkaHealth,
                               metabase: MetabaseHealth,
                               timestamp: String
                             )

class HealthCheckService(config: AppConfig)(
  implicit system: ActorSystem,
  mat: Materializer,
  ec: ExecutionContext
) {

  private val http = Http(system)

  // ---------------------------------------------
  //                     FLINK
  // ---------------------------------------------
  def checkFlink(): Future[FlinkHealth] = {
    val base = config.flink.restApiUrl

    val overviewF = http.singleRequest(HttpRequest(uri = s"$base/overview"))
      .flatMap(_.entity.toStrict(5.seconds))
      .map(_.data.utf8String.parseJson)
      .map { json =>
        val obj = json.asJsObject
        val tm  = obj.fields("taskmanagers").convertTo[Int]
        val st  = obj.fields("slots-total").convertTo[Int]
        val sa  = obj.fields("slots-available").convertTo[Int]
        FlinkClusterHealth(
          status = if (sa <= st) "HEALTHY" else "UNHEALTHY",
          taskmanagers = tm,
          slotsTotal = st,
          slotsAvailable = sa
        )
      }

    val jobsF = http.singleRequest(HttpRequest(uri = s"$base/jobs/overview"))
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

    for {
      overview <- overviewF
      jobs <- jobsF
    } yield FlinkHealth(overview, jobs)
  }

  // ---------------------------------------------
  //                   METABASE
  // ---------------------------------------------
  def checkMetabase(): Future[MetabaseHealth] = {
    val url = s"${config.metabase.url}/api/health"

    http.singleRequest(HttpRequest(uri = url))
      .map(resp => MetabaseHealth(
        if (resp.status.isSuccess()) "HEALTHY" else "UNHEALTHY",
        config.metabase.url
      ))
  }

  // ---------------------------------------------
  //                   KAFKA
  // ---------------------------------------------
  def checkKafka(): Future[KafkaHealth] = Future {
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", config.kafka.broker)
    producerProps.put("key.serializer", classOf[StringSerializer].getName)
    producerProps.put("value.serializer", classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](producerProps)
    val topic = "_healthcheck_test"
    val uuid = UUID.randomUUID().toString

    producer.send(new ProducerRecord(topic, uuid))
    producer.flush()
    producer.close()

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.broker)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "hc-" + UUID.randomUUID())
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(Collections.singletonList(topic))

    val records = consumer.poll(java.time.Duration.ofSeconds(3))
    consumer.close()

    val ok = records.iterator().asScala.exists(_.value() == uuid)

    KafkaHealth(if (ok) "HEALTHY" else "UNHEALTHY", config.kafka.broker)
  }

  // ---------------------------------------------
  //                FULL HEALTH RESPONSE
  // ---------------------------------------------
  def fullHealth(): Future[FullHealthResponse] = {
    for {
      flink <- checkFlink()
      kafka <- checkKafka()
      meta  <- checkMetabase()
    } yield FullHealthResponse(
      flink, kafka, meta, Instant.now.toString
    )
  }
}

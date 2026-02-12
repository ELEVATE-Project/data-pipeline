package org.shikshalokam.job.akkaservice.services

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.shikshalokam.job.akkaservice.functions.Functions
import org.shikshalokam.job.akkaservice.models.JsonProtocol._
import org.shikshalokam.job.akkaservice.models._
import spray.json._

import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object HealthCheckService {

  private val config = Functions.load()

  def checkFlink()(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext): Future[FlinkHealth] = {
    val http = Http()
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

  def checkMetabase()(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext): Future[MetabaseHealth] = {
    val http = Http()
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

  def checkKafka()(implicit system: ActorSystem, ec: ExecutionContext): Future[KafkaHealth] = {
    val blockingEc = system.dispatchers.lookup("blocking-io-dispatcher")
    
    Future {
      var adminClient: AdminClient = null
      try {
        val props = new Properties()
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafka.broker)
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
        
        adminClient = AdminClient.create(props)
        
        // Simple connectivity check - describe cluster
        val result = adminClient.describeCluster()
        val clusterId = result.clusterId().get(10, TimeUnit.SECONDS)
        
        KafkaHealth("HEALTHY", config.kafka.broker)
      } catch {
        case _: Exception => KafkaHealth("UNHEALTHY", config.kafka.broker)
      } finally {
        if (adminClient != null) {
          try {
            adminClient.close(java.time.Duration.ofSeconds(5))
          } catch {
            case _: Exception => // Ignore close errors
          }
        }
      }
    }(blockingEc) // Run on dedicated blocking dispatcher
  }

  def fullHealth()(implicit system: ActorSystem, mat: Materializer, ec: ExecutionContext): Future[FullHealthResponse] = {
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

package org.shikshalokam.job.akkaservice.services

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{FileIO, Framing, Sink}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.shikshalokam.job.akkaservice.models._
import org.shikshalokam.job.akkaservice.services.Service.materializer.executionContext
import spray.json.DefaultJsonProtocol._
import spray.json._
import java.nio.file.{Files, Path, Paths}
import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}
import org.shikshalokam.job.akkaservice.config.AppConfig

object Service {

  implicit val system: ActorSystem = ActorSystem("user-upload-system")
  implicit val materializer: Materializer = Materializer(system)

  private val config = AppConfig.config
  private val sinkDirectory = config.getString("akka.file.sinkDirectory")
  private val flinkBase = config.getString("flink.url")
  private val metaUrl = s"${config.getString("metabase.url")}/health"
  private val broker = config.getString("kafka.broker.servers")
  private val configuredJobs = config.getStringList("flink.jobs").asScala.toList
  private val http = Http()
  private val debugMode = config.getString("akka.debug.logs").contains("true")

  def processCsvUpload(formData: Multipart.FormData): Future[String] = {
    val filePartFuture: Future[Option[Multipart.FormData.BodyPart]] = formData.parts
      .mapAsync(1) { part =>
        if (part.name == "file") {
          Future.successful(Some(part))
        } else {
          part.entity.discardBytes()
          Future.successful(None)
        }
      }
      .runFold(Option.empty[Multipart.FormData.BodyPart]) { (acc, part) => acc.orElse(part) }

    filePartFuture.flatMap {
      case Some(part) =>
        val filename = part.filename.getOrElse("uploaded.csv")
        if (!filename.toLowerCase.endsWith(".csv")) {
          println("Upload a valid CSV file")
          throw new Exception("Upload a valid CSV file")
        } else {
          val headerValidationSource = part.entity.dataBytes
            .via(Framing.delimiter(ByteString("\n"), 1024, allowTruncation = true))
            .map(_.utf8String)
            .take(1)
          headerValidationSource.runWith(Sink.head).flatMap { headerLine =>
            val headers = headerLine.split(",").map(_.trim.toLowerCase).toList
            val expectedHeaders = CsvSchema.headers.map(_.toLowerCase)
            if (headers != expectedHeaders) {
              println("CSV headers do not match expected format")
              Future.failed(new Exception("CSV headers do not match expected format"))
            } else {
              val currentTimestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss"))
              val newFilename = s"$currentTimestamp.csv"
              val filePath: Path = Paths.get(sinkDirectory, newFilename)
              Files.createDirectories(filePath.getParent)
              val sink: Sink[ByteString, Future[IOResult]] = FileIO.toPath(filePath)
              part.entity.dataBytes.runWith(sink).map(_ => newFilename)
            }
          }
        }
      case None =>
        println("No file uploaded")
        throw new Exception("No file uploaded")
    }
  }

  def listUploadedCsvFiles: List[String] = {
    Try {
      Files.walk(Paths.get(sinkDirectory))
        .iterator()
        .asScala
        .filter(path => Files.isRegularFile(path) && path.toString.toLowerCase.endsWith(".csv"))
        .map(_.getFileName.toString)
        .toList
    } match {
      case Success(filesList) => filesList
      case Failure(exception) =>
        throw new Exception("Error reading directory or no CSV files found: ")
    }
  }

  def checkFlink(): Future[FlinkHealth] = {

    val unhealthyCluster = FlinkClusterHealth("UNHEALTHY", 0, 0, 0, 0, 0, 0, 0, "")

    val overviewFlink = http.singleRequest(HttpRequest(uri = s"$flinkBase/overview"))
      .flatMap(_.entity.toStrict(5.seconds))
      .map(_.data.utf8String.parseJson)
      .map { json =>
        val obj = json.asJsObject
        val tm = obj.fields("taskmanagers").convertTo[Int]
        val st = obj.fields("slots-total").convertTo[Int]
        val sa = obj.fields("slots-available").convertTo[Int]
        val jr = obj.fields("jobs-running").convertTo[Int]
        val jf = obj.fields("jobs-finished").convertTo[Int]
        val jc = obj.fields("jobs-cancelled").convertTo[Int]
        val jfailed = obj.fields("jobs-failed").convertTo[Int]
        val fv = obj.fields("flink-version").convertTo[String]
        FlinkClusterHealth(
          status = if (tm > 0 && sa >= 0) "HEALTHY" else "UNHEALTHY",
          taskmanagers = tm,
          slotsTotal = st,
          slotsAvailable = sa,
          jobsRunning = jr,
          jobsFinished = jf,
          jobsCancelled = jc,
          jobsFailed = jfailed,
          flinkVersion = fv
        )
      }
      .recover { case e: Exception =>
        println(s"Error fetching/parsing Flink overview: ${e.getMessage}")
        e.printStackTrace()
        unhealthyCluster
      }

    val jobsFlink = http.singleRequest(HttpRequest(uri = s"$flinkBase/jobs/overview"))
      .flatMap(_.entity.toStrict(5.seconds))
      .map(_.data.utf8String.parseJson)
      .map { json =>
        val jobArray = json.asJsObject.fields("jobs").convertTo[List[JsObject]]

        // Check all configured job names and extract state directly from overview
        configuredJobs.map { jobName =>
          jobArray.find(_.fields("name").convertTo[String] == jobName) match {
            case Some(jobObj) =>
              val state = jobObj.fields("state").convertTo[String]
              FlinkJobStatus(jobName, state)
            case None =>
              FlinkJobStatus(jobName, "NOT_FOUND")
          }
        }
      }
      .recover { case _ => configuredJobs.map(j => FlinkJobStatus(j, "UNREACHABLE")) }

    for {
      overview <- overviewFlink
      jobs <- jobsFlink
    } yield FlinkHealth(overview, jobs)
  }

  def checkMetabase(): Future[MetabaseHealth] = {

    http.singleRequest(HttpRequest(uri = metaUrl))
      .flatMap { resp =>
        resp.entity.discardBytes()
        Future.successful(MetabaseHealth(
          if (resp.status.isSuccess()) "HEALTHY" else "UNHEALTHY",
          metaUrl
        ))
      }
      .recover { case _ => MetabaseHealth("UNHEALTHY", metaUrl) }
  }

  def checkKafka(): Future[KafkaHealth] = {
    val blockingEc = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

    Future {
      val pid = java.lang.management.ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
      val uniqueTopicName = s"health-check-$pid"
      val uniqueGroupId = s"health-check-group-$pid"

      var adminClient: AdminClient = null
      var producer: org.apache.kafka.clients.producer.KafkaProducer[String, String] = null
      var consumer: org.apache.kafka.clients.consumer.KafkaConsumer[String, String] = null

      try {
        if (debugMode) {
          println(s"[Kafka Health Check] Connecting to Kafka at ${broker}")
        }

        // Step 1: Create AdminClient and check connectivity
        val adminProps = new Properties()
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
        adminClient = AdminClient.create(adminProps)

        val clusterId = adminClient.describeCluster().clusterId().get(10, TimeUnit.SECONDS)
        if (debugMode) {
          println(s"[Kafka Health Check] Connected to cluster: $clusterId ✅")
        }

        // Step 2: Ensure topic exists or create it
        val existingTopics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS)
        if (!existingTopics.contains(uniqueTopicName)) {
          if (debugMode) {
            println(s"[Kafka Health Check] Creating topic '$uniqueTopicName'... ⏳")
          }
          val newTopic = new org.apache.kafka.clients.admin.NewTopic(uniqueTopicName, 1, 1.toShort)
          adminClient.createTopics(java.util.Collections.singletonList(newTopic))
            .all()
            .get(10, TimeUnit.SECONDS)
          if (debugMode) {
            println(s"[Kafka Health Check] Topic created ✅")
          }
        }

        // Step 3: Create producer and send message
        val producerProps = new Properties()
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all")
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
        producer = new org.apache.kafka.clients.producer.KafkaProducer[String, String](producerProps)

        val messageId = s"health-check-${java.util.UUID.randomUUID()}"
        val record = new org.apache.kafka.clients.producer.ProducerRecord[String, String](uniqueTopicName, messageId)
        producer.send(record).get(5, TimeUnit.SECONDS)
        if (debugMode) {
          println(s"[Kafka Health Check] Sent message: $messageId ✅")
        }

        // Step 4: Create consumer and receive message
        val consumerProps = new Properties()
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupId)
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringDeserializer].getName)
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.StringDeserializer].getName)
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        consumerProps.put(org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
        consumer = new org.apache.kafka.clients.consumer.KafkaConsumer[String, String](consumerProps)

        consumer.subscribe(java.util.Collections.singletonList(uniqueTopicName))

        val startTime = System.currentTimeMillis()
        val timeout = 5000 // 5 seconds
        var messageReceived = false

        while (!messageReceived && (System.currentTimeMillis() - startTime) < timeout) {
          val records = consumer.poll(java.time.Duration.ofMillis(1000))
          records.asScala.foreach { record =>
            if (record.value() == messageId) {
              messageReceived = true
              if (debugMode) {
                println(s"[Kafka Health Check] Message received ✅")
              }
            }
          }
        }

        if (!messageReceived) {
          if (debugMode) {
            println("[Kafka Health Check] Message not received in time ❌")
          }
          // Cleanup topic before returning
          try {
            adminClient.deleteTopics(java.util.Collections.singletonList(uniqueTopicName))
              .all()
              .get(10, TimeUnit.SECONDS)
          } catch {
            case _: Exception => // Ignore cleanup errors
          }
          KafkaHealth("UNHEALTHY", broker)
        } else {
          // Step 5: Cleanup - delete the topic
          try {
            if (debugMode) {
              println(s"[Kafka Health Check] Deleting topic '$uniqueTopicName'... ⏳")
            }
            adminClient.deleteTopics(java.util.Collections.singletonList(uniqueTopicName))
              .all()
              .get(10, TimeUnit.SECONDS)
            if (debugMode) {
              println(s"[Kafka Health Check] Topic deleted ✅")
            }
          } catch {
            case ex: Exception =>
              if (debugMode) {
                println(s"[Kafka Health Check] Failed to delete topic: ${ex.getMessage}")
              }
          }
          KafkaHealth("HEALTHY", broker)
        }

      } catch {
        case ex: Exception =>
          if (debugMode) {
            println(s"[Kafka Health Check] Health check failed: ${ex.getMessage}")
            ex.printStackTrace()
          }
          // Try to cleanup topic even on failure
          try {
            if (adminClient != null) {
              adminClient.deleteTopics(java.util.Collections.singletonList(uniqueTopicName))
                .all()
                .get(10, TimeUnit.SECONDS)
            }
          } catch {
            case _: Exception => // Ignore cleanup errors
          }
          KafkaHealth("UNHEALTHY", broker)
      } finally {
        // Cleanup resources
        if (consumer != null) {
          try {
            consumer.close()
          } catch {
            case _: Exception =>
          }
        }
        if (producer != null) {
          try {
            producer.close()
          } catch {
            case _: Exception =>
          }
        }
        if (adminClient != null) {
          try {
            adminClient.close(java.time.Duration.ofSeconds(5))
          } catch {
            case _: Exception =>
          }
        }
      }
    }(blockingEc) // Run on dedicated blocking dispatcher
  }

  def fullHealth(): Future[FullHealthResponse] = {
    val flink = checkFlink()
    val kafka = checkKafka()
    val meta = checkMetabase()

    for {
      flink <- flink
      kafka <- kafka
      meta <- meta
    } yield FullHealthResponse(
      flink, kafka, meta, Instant.now.toString
    )
  }
}


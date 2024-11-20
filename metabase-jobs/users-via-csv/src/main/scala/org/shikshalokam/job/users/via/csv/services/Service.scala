package org.shikshalokam.job.users.via.csv.services

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Multipart
import akka.stream.scaladsl.{FileIO, Framing, Sink}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import org.shikshalokam.job.users.via.csv.models.CsvSchema
import org.shikshalokam.job.users.via.csv.services.Service.materializer.executionContext

import java.nio.file.{Files, Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object Service {

  implicit val system: ActorSystem = ActorSystem("user-upload-system")
  implicit val materializer: Materializer = Materializer(system)

  private val config = ConfigFactory.load()
  private val sinkDirectory = config.getString("file.sinkDirectory")

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

}


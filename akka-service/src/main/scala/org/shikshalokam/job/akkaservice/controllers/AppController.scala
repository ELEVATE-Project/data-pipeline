package org.shikshalokam.job.akkaservice.controllers

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.shikshalokam.job.akkaservice.models.FileNames
import org.shikshalokam.job.akkaservice.models.JsonProtocol._
import org.shikshalokam.job.akkaservice.services.Service.materializer.system
import org.shikshalokam.job.akkaservice.services.{CsvProcessingActor, Service}
import spray.json._

trait CsvJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val fileNamesFormat = jsonFormat1(FileNames)
}

object AppController extends CsvJsonProtocol {

  private val csvProcessingActor: ActorRef = system.actorOf(CsvProcessingActor.props, "csvProcessingActor")

  // CSV Routes Controller
  def uploadCsvFile: Route =
    entity(as[Multipart.FormData]) { formData =>
      onComplete(Service.processCsvUpload(formData)) {
        case scala.util.Success(filename) =>
          println(s"CSV file uploaded successfully as: $filename")
          csvProcessingActor ! filename
          complete(StatusCodes.OK, s"CSV file uploaded successfully as: $filename")
        case scala.util.Failure(ex) =>
          complete(StatusCodes.InternalServerError, s"Failed to upload file: ${ex.getMessage}")
      }
    }

  def listUploadedCsvFiles: Route = {
    try {
      val files = Service.listUploadedCsvFiles
      complete(HttpResponse(StatusCodes.OK, entity = files.mkString("\n")))
    } catch {
      case e: Exception =>
        complete(HttpResponse(StatusCodes.InternalServerError, entity = e.getMessage))
    }
  }

  // Health Check Controller
  def healthCheck: Route = {
    try {
      onSuccess(Service.fullHealth()) { data =>
        complete(HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`application/json`, data.toJson.prettyPrint)))
      }
    } catch {
      case e: Exception =>
        complete(HttpResponse(StatusCodes.InternalServerError, entity = e.getMessage))
    }
  }

  def serviceHealthCheck(service: String): Route = {
    service.toLowerCase match {
      case "flink" =>
        onSuccess(Service.checkFlink()) { health =>
          complete(HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`application/json`, health.toJson.prettyPrint)))
        }
      case "kafka" =>
        onSuccess(Service.checkKafka()) { health =>
          complete(HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`application/json`, health.toJson.prettyPrint)))
        }
      case "metabase" =>
        onSuccess(Service.checkMetabase()) { health =>
          complete(HttpResponse(StatusCodes.OK, entity = HttpEntity(ContentTypes.`application/json`, health.toJson.prettyPrint)))
        }
      case _ =>
        complete(StatusCodes.NotFound, s"Unknown service: $service")
    }
  }
}

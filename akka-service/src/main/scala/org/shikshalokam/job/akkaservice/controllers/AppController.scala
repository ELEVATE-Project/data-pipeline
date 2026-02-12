package org.shikshalokam.job.akkaservice.controllers

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpResponse, Multipart, StatusCodes, HttpEntity, ContentTypes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.shikshalokam.job.akkaservice.models.FileNames
import org.shikshalokam.job.akkaservice.services.Service.materializer.system
import org.shikshalokam.job.akkaservice.services.{CsvProcessingActor, Service, HealthCheckService}
import org.shikshalokam.job.akkaservice.models.JsonProtocol._
import spray.json._
import scala.concurrent.ExecutionContext

trait CsvJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val fileNamesFormat = jsonFormat1(FileNames)
}

object AppController extends CsvJsonProtocol {

  private val csvProcessingActor: ActorRef = system.actorOf(CsvProcessingActor.props, "csvProcessingActor")

  // CSV Routes Logic
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

  // Health Check Logic
  def healthCheck(implicit system: ActorSystem, mat: akka.stream.Materializer, ec: ExecutionContext): Route = {
    onSuccess(HealthCheckService.fullHealth()) { data =>
      complete(HttpEntity(ContentTypes.`application/json`, data.toJson.prettyPrint))
    }
  }
}

package org.shikshalokam.job.users.via.csv.controllers

import akka.actor.ActorRef
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpResponse, Multipart, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.shikshalokam.job.users.via.csv.models.FileNames
import org.shikshalokam.job.users.via.csv.services.Service.materializer.system
import org.shikshalokam.job.users.via.csv.services.{CsvProcessingActor, Service}
import spray.json._


trait CsvJsonProtocol extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val fileNamesFormat = jsonFormat1(FileNames)
}

object Controller extends CsvJsonProtocol {

  private val csvProcessingActor: ActorRef = system.actorOf(CsvProcessingActor.props, "csvProcessingActor")

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

}

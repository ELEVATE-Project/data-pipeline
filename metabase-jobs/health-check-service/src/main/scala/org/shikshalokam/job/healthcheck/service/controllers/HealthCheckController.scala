package org.shikshalokam.job.healthcheck.service.controllers

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.shikshalokam.job.healthcheck.service.services.HealthCheckService
import org.shikshalokam.job.healthcheck.service.models.JsonProtocol._
import spray.json._
import scala.concurrent.ExecutionContext

object HealthCheckController {

  implicit val ec: ExecutionContext = HealthCheckService.ec

  def checkHealth: Route = {
    parameter("basicCheck".?) { _ =>
      complete {
        HealthCheckService.fullHealth().map(_.toJson.prettyPrint)
      }
    }
  }
}

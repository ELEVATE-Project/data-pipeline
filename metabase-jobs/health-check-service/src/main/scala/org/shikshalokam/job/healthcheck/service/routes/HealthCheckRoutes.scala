package org.shikshalokam.job.healthcheck.service.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import org.shikshalokam.job.healthcheck.service.services.HealthCheckService
import spray.json._
import org.shikshalokam.job.healthcheck.service.models.JsonProtocol._
import org.shikshalokam.job.healthcheck.service.functions.AppConfigLoader

object HealthCheckRoutes {

  private val config = AppConfigLoader.load()

  val route =
    path("health") {
      extractRequest { req =>
        val tokenOpt = req.getHeader("X-API-KEY")

        val providedToken =
          if (tokenOpt.isPresent) tokenOpt.get.value()
          else ""

        if (providedToken != config.security.apiToken) {
          complete(HttpResponse(
            status = StatusCodes.Unauthorized,
            entity = "Invalid or missing API token"
          ))
        } else {
          onSuccess(HealthCheckService.fullHealth()) { data =>
            complete(HttpEntity(ContentTypes.`application/json`, data.toJson.prettyPrint))
          }
        }
      }
    }
}

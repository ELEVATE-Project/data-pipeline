package org.shikshalokam.job.healthcheck.service.routes

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import org.shikshalokam.job.healthcheck.service.services.HealthCheckService
import spray.json._
import org.shikshalokam.job.healthcheck.service.models.JsonProtocol._
import org.shikshalokam.job.healthcheck.service.functions.AppConfigLoader

import java.security.MessageDigest
import scala.concurrent.ExecutionContext

object HealthCheckRoutes {

  private val config = AppConfigLoader.load()

  private def secureEquals(a: String, b: String): Boolean = {
    MessageDigest.isEqual(a.getBytes("UTF-8"), b.getBytes("UTF-8"))
  }

  def route(implicit ec: ExecutionContext) =
    path("health") {
      get {
        extractRequest { req =>
          val tokenOpt = req.getHeader("X-API-KEY")

          val providedToken =
            if (tokenOpt.isPresent) tokenOpt.get.value()
            else ""

          if (!secureEquals(providedToken, config.security.apiToken)) {
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
}

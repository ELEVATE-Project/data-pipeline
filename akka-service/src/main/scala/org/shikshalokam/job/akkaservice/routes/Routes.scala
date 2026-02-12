package org.shikshalokam.job.akkaservice.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.ConfigFactory
import org.shikshalokam.job.akkaservice.controllers.AppController
import akka.http.scaladsl.model._
import java.security.MessageDigest

object Routes {

  private val config = ConfigFactory.load()
  private val apiToken = config.getString("security.api-token")

  /*
   * Unified route definition for /api
   * Includes:
   *  - /api/csv (Requires Authorization header)
   *  - /api/health (Requires X-API-KEY header)
   */
  def route(implicit ec: scala.concurrent.ExecutionContext): Route =
    pathPrefix("api") {
      concat(
        // CSV Routes
        pathPrefix("csv") {
          headerValueByName("Authorization") { token =>
            if (token == apiToken) {
              concat(
                path("upload") {
                  post(AppController.uploadCsvFile)
                },
                path("list") {
                  get(AppController.listUploadedCsvFiles)
                }
              )
            } else {
              complete((StatusCodes.Unauthorized, "Invalid or missing token"))
            }
          }
        },
        // Health Check Routes
        path("health") {
          get {
            extractRequest { req =>
              val tokenOpt = req.getHeader("X-API-KEY")
              val providedToken = if (tokenOpt.isPresent) tokenOpt.get.value() else ""

              if (!secureEquals(providedToken, apiToken)) {
                complete(HttpResponse(
                  status = StatusCodes.Unauthorized,
                  entity = "Invalid or missing API token"
                ))
              } else {
                AppController.healthCheck(ec)
              }
            }
          }
        }
      )
    }

  private def secureEquals(a: String, b: String): Boolean = {
    MessageDigest.isEqual(a.getBytes("UTF-8"), b.getBytes("UTF-8"))
  }
}


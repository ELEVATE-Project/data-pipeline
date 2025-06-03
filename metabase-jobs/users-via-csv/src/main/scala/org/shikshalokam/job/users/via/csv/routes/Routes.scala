package org.shikshalokam.job.users.via.csv.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.config.ConfigFactory
import org.shikshalokam.job.users.via.csv.controllers.Controller

object Routes {

  private val config = ConfigFactory.load()
  private val apiToken = config.getString("security.api-token")

  val route: Route =
    pathPrefix("api") {
      headerValueByName("Authorization") { token =>
        if (token == apiToken) {
          pathPrefix("csv") {
            concat(
              path("upload") {
                post(Controller.uploadCsvFile)
              },
              path("list") {
                get(Controller.listUploadedCsvFiles)
              }
            )
          }
        } else {
          complete((StatusCodes.Unauthorized, "Invalid or missing token"))
        }
      }
    }
}


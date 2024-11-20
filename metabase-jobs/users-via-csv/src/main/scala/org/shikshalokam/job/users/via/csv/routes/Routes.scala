package org.shikshalokam.job.users.via.csv.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.shikshalokam.job.users.via.csv.controllers.Controller

object Routes {
  val route: Route =
    pathPrefix("api") {
      concat(
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
      )
    }
}


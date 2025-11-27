package org.shikshalokam.job.healthcheck.service.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.shikshalokam.job.healthcheck.service.controllers.HealthCheckController

object HealthCheckRoutes {

  val route: Route =
    path("health") {
      get(HealthCheckController.checkHealth)
    }
}

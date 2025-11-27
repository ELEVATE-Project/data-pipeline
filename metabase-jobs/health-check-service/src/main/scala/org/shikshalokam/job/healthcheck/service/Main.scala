package org.shikshalokam.job.healthcheck.service

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import org.shikshalokam.job.healthcheck.service.routes.HealthCheckRoutes
import scala.concurrent.ExecutionContextExecutor

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("healthcheck-system")
    implicit val mat: Materializer = Materializer(system)
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val route = HealthCheckRoutes.route

    Http().newServerAt("0.0.0.0", 8080).bind(route)

    println("HealthCheck Service running at http://localhost:8080/health")
  }
}

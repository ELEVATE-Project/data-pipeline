package org.shikshalokam.job.healthcheckservice

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.Materializer
import spray.json._
import scala.concurrent.ExecutionContextExecutor

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("healthcheck-system")
    implicit val mat: Materializer  = Materializer(system)
    implicit val ec: ExecutionContextExecutor = system.dispatcher

    val config = AppConfigLoader.load()
    val service = new HealthCheckService(config)

    import JsonProtocol._

    val route =
      path("health") {
        get {
          parameter("basicCheck".?) { _ =>
            complete {
              service.fullHealth().map(_.toJson.prettyPrint)
            }
          }
        }
      }

    Http().newServerAt("0.0.0.0", 8080).bind(route)

    println("HealthCheck Service running at http://localhost:8080/health")
  }
}

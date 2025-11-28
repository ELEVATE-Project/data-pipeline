package org.shikshalokam.job.healthcheck.service

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import com.typesafe.config.ConfigFactory
import org.shikshalokam.job.healthcheck.service.routes.HealthCheckRoutes

import scala.concurrent.ExecutionContextExecutor

object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("healthcheck-system")
    implicit val mat: Materializer = Materializer(system)
    implicit val ec: ExecutionContextExecutor = system.dispatcher
    val config = ConfigFactory.load()
    val host = config.getString("akka.http.host")
    val port = config.getInt("akka.http.port")

    val route = HealthCheckRoutes.route

    Http().newServerAt("0.0.0.0", 8080).bind(route).onComplete {
      case scala.util.Success(binding) =>
        println(s"HealthCheck Service running at http://${host}:${port}/health")
        sys.addShutdownHook {
          binding.unbind().onComplete(_ => system.terminate())
        }
      case scala.util.Failure(ex) =>
        println(s"Failed to bind HTTP server: ${ex.getMessage}")
        system.terminate()
    }
  }
}

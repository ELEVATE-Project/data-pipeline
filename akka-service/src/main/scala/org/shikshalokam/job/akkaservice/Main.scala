package org.shikshalokam.job.akkaservice

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import org.shikshalokam.job.akkaservice.routes.Routes
import com.typesafe.config.ConfigFactory
import akka.http.scaladsl.server.Route

import scala.concurrent.duration.Duration
import scala.concurrent.Await

object Main extends App {
  implicit val system = ActorSystem("akka-http-server")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  private val config = ConfigFactory.load()
  private val host = config.getString("akka.http.host")
  private val port = config.getInt("akka.http.port")

  // Routes.route now includes both CSV and Health checks under /api
  val routes: Route = Routes.route

  val bindingFuture = Http().newServerAt(host, port).bind(routes)

  println(s"Server online at http://${host}:${port}/")

  sys.addShutdownHook {
    println("Shutting down server...")
    bindingFuture
      .flatMap(_.unbind())
      .onComplete { _ =>
        println("Actor system terminated.")
        system.terminate()
      }
  }

  Await.result(system.whenTerminated, Duration.Inf)
}

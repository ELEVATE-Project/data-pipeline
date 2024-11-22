package org.shikshalokam.job.users.via.csv

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server.Route
import com.typesafe.config.ConfigFactory
import org.shikshalokam.job.users.via.csv.routes.Routes

import scala.concurrent.duration.Duration
import scala.concurrent.Await

object Main extends App {
  implicit val system = ActorSystem("akka-http-server")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  private val config = ConfigFactory.load()
  private val host = config.getString("akka.http.host")
  private val port = config.getInt("akka.http.port")

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

package org.uam.masterbigdata

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.Materializer
import org.uam.masterbigdata.configuration.ApplicationConfiguration

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebApplication extends App with ApplicationConfiguration {
  // A K K A   A C T O R   S Y S T E M
  implicit lazy val system: ActorSystem = ActorSystem("WebActorSystem")
  implicit val executionContext: ExecutionContext = system.dispatcher
  implicit lazy val materializer: Materializer = Materializer(system)
  system.log.info(s"Starting VertexComposer WebApplication")

  // L A U N C H  W E B  S E R V E R (actor based)
  val futureBinding: Future[Http.ServerBinding] =
    Http()
      .newServerAt(serverAddress, serverPort)
      .bind(routes)
}

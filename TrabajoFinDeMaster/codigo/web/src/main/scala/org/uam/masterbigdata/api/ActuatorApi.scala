package org.uam.masterbigdata.api

import org.uam.masterbigdata.api.documentation.ActuatorEndpoint
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.directives.DebuggingDirectives
import org.uam.masterbigdata.api.Converters.buildInfoToApi
import sttp.tapir.server.akkahttp._

import scala.concurrent.Future

/**
 * Actuator endpoint for monitoring application
 * http://host:port/api/v1.0/health
 */
trait ActuatorApi {

  // https://doc.akka.io/docs/akka-http/current/routing-dsl/directives/debugging-directives/logRequestResult.html
  val route: Route = DebuggingDirectives.logRequestResult("actuator-logger") {
    ActuatorEndpoint.healthEndpoint.toRoute { _ =>
      Future.successful(Right(buildInfoToApi()))
    }
  }

}

object ActuatorApi extends ActuatorApi

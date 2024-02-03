package org.uam.masterbigdata.api

import org.uam.masterbigdata.api.documentation.{ActuatorEndpoint, ApiEndpoint, EventsEndpoint, FramesEndpoint, JourneysEndpoint}
import org.uam.masterbigdata.api.model.BuildInfo
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import sttp.tapir.docs.openapi._
import sttp.tapir.openapi.circe.yaml._
trait SwaggerApi {
  /* Esta dependencia es transitiva de Tapir
     Si no se genera el Swagger verificar con 'dependencyTree' que es la misma versión
     Ex: 'org.webjars:swagger-ui:3.24.3'
   */
  private val swaggerUiVersion = "3.25.0"

  private lazy val endpoints = Seq(
    // A C T U A T O R  E N D P O I N T
    ActuatorEndpoint.healthEndpoint,
    // J O U R N E Y S
    JourneysEndpoint.getJourneyByIdEndpoint,
    JourneysEndpoint.getJourneysByLabelEndpoint,
    JourneysEndpoint.getJourneysEndpoint,
    // E V E N T S
    EventsEndpoint.getEventByIdEndpoint,
    EventsEndpoint.getEventsEndpoint,
    // F R A M E S
    //FramesEndpoint.getFrameByIdEndpoint,
    //FramesEndpoint.getFramesEndpoint
  )

  private lazy val docsAsYaml: String =
    endpoints
      .toOpenAPI(BuildInfo.name, BuildInfo.version)
      .toYaml

  private lazy val contextPath = "docs"
  private lazy val yamlName = "docs.yaml"

  lazy val route: Route = pathPrefix(ApiEndpoint.apiResource / ApiEndpoint.apiVersion) {
    pathPrefix(contextPath) {
      pathEndOrSingleSlash {
        redirect(s"$contextPath/index.html?url=/${ApiEndpoint.apiResource}/${ApiEndpoint.apiVersion}/$contextPath/$yamlName", StatusCodes.PermanentRedirect)
      } ~ path(yamlName) {
        complete(docsAsYaml)
      } ~ getFromResourceDirectory(s"META-INF/resources/webjars/swagger-ui/$swaggerUiVersion/")
    }
  }

}

object SwaggerApi extends SwaggerApi

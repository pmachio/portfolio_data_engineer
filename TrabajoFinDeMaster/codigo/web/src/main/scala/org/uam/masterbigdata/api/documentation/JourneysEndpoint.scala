package org.uam.masterbigdata.api.documentation



import org.uam.masterbigdata.api.documentation.ApiEndpoint.{baseEndpoint, journeysResource, objectIdPath}
import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.error.DomainError
import sttp.tapir.{Endpoint, EndpointInput, _}

trait JourneysEndpoint extends JsonCodecs  with ApiErrorMapping {

  private lazy  val journeyByIdResource: EndpointInput[(Long, String)]= journeysResource / objectIdPath
  lazy val getJourneyByIdEndpoint: Endpoint[(Long, String), DomainError, JourneyView, Nothing] =
    baseEndpoint
      .get
      .name("GetJourneyById")
      .description("Recupera un trayecto por su identificador")
      .in(journeyByIdResource)
      .out(jsonBody[JourneyView])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))

  private lazy val objectLabelPath = path[String]("label")
  private lazy val journeyByNameResource: EndpointInput[(Long, String)] = journeysResource / "label" / objectLabelPath
  lazy val getJourneysByLabelEndpoint: Endpoint[(Long, String), DomainError, Seq[JourneyView], Nothing] =
    baseEndpoint
      .get
      .name("GetJourneyByLabel")
      .description("Recupera todos los trayectos que tienen la misma etiqueta")
      .in(journeyByNameResource)
      .out(jsonBody[Seq[JourneyView]])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))

  lazy val getJourneysEndpoint: Endpoint[Long, DomainError, Seq[JourneyView], Nothing] =
    baseEndpoint
      .get
      .name("GetAllJourneys")
      .description("Recupera todos los trayectos")
      .in(journeysResource)
      .out(jsonBody[Seq[JourneyView]])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))
}

object JourneysEndpoint extends JourneysEndpoint{

}
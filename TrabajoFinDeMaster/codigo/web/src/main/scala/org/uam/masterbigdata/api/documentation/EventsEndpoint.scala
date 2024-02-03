package org.uam.masterbigdata.api.documentation

import org.uam.masterbigdata.api.documentation.ApiEndpoint.{baseEndpoint, objectIdPath, eventsResource}
import org.uam.masterbigdata.domain.AdapterModel.EventView
import org.uam.masterbigdata.domain.model.error.DomainError
import sttp.tapir.{Endpoint, EndpointInput, _}

trait EventsEndpoint extends JsonCodecs  with ApiErrorMapping {

  private[api] lazy val eventIdPath = path[String]("eventId")
  private lazy val eventByIdResource: EndpointInput[(Long, String)] = eventsResource / eventIdPath
  lazy val getEventByIdEndpoint: Endpoint[(Long, String), DomainError, EventView, Nothing] =
    baseEndpoint
      .get
      .name("GetEventById")
      .description("Recupera un evento por su identificador")
      .in(eventByIdResource)
      .out(jsonBody[EventView])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))


  lazy val getEventsEndpoint: Endpoint[Long, DomainError, Seq[EventView], Nothing] =
    baseEndpoint
      .get
      .name("GetAllEvents")
      .description("Recupera todos los eventos de un dispositivo")
      .in(eventsResource)
      .out(jsonBody[Seq[EventView]])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))
}

object EventsEndpoint extends EventsEndpoint {}


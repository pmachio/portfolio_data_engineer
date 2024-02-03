package org.uam.masterbigdata.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.api.documentation.{ApiMapper, EventsEndpoint}
import org.uam.masterbigdata.domain.AdapterModel.EventView
import org.uam.masterbigdata.domain.model.error.DomainError
import org.uam.masterbigdata.domain.service.EventsService
import sttp.tapir.server.akkahttp.RichAkkaHttpEndpoint
import org.uam.masterbigdata.domain.model.Entities.Event

import scala.concurrent.Future
import scala.util.{Failure, Success}

import scala.concurrent.ExecutionContext.Implicits.global

class EventsApi(service:EventsService) extends ApiMapper with ComponentLogging {

  import ApiHelpers._

  lazy val routes: Route = getEventByIdRoute ~ getAllEventsRoute

  private lazy val getEventByIdRoute: Route = EventsEndpoint.getEventByIdEndpoint.toRoute {
    mapToDeviceEventRequest andThen service.getDeviceEvent andThen handleEventResult
  }
  private lazy val getAllEventsRoute: Route = EventsEndpoint.getEventsEndpoint.toRoute {
    mapToDeviceEventsRequest andThen service.getAllDeviceEvents andThen handleTagFeatureVersionResultSeq
  }

  private lazy val handleEventResult: Future[Event] => Future[Either[DomainError, EventView]] = future => future.transform {
    case Success(value) => makeSuccessResult(Converters.modelToApi(value))
    case Failure(exception) =>
      log.error(exception.getMessage)
      exception match {
        case e: DomainError => makeFailureResult(e)
        case _ => makeFailureResult(DomainError.UnexpectedServiceError)
      }
  }

  private lazy val handleTagFeatureVersionResultSeq: Future[Seq[Event]] => Future[Either[DomainError, Seq[EventView]]] = future => future.transform {
    case Success(value) => makeSuccessResult(value.map(Converters.modelToApi))
    case Failure(exception) =>
      log.error(exception.getMessage)
      exception match {
        case e: DomainError => makeFailureResult(e)
        case _ => makeFailureResult(DomainError.UnexpectedServiceError)
      }
  }
}

object EventsApi{
    def apply(service:EventsService) = new EventsApi(service)
}

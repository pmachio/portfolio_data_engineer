package org.uam.masterbigdata.api

import org.uam.masterbigdata.api.documentation.{ApiMapper, JourneysEndpoint}
import org.uam.masterbigdata.domain.service.JourneysService
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.domain.AdapterModel.JourneyView
import org.uam.masterbigdata.domain.model.Entities.Journey
import org.uam.masterbigdata.domain.model.error.DomainError
import sttp.tapir.server.akkahttp._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}



class JourneysApi(service:JourneysService) extends ApiMapper with ComponentLogging {
  import ApiHelpers._
  lazy val routes: Route = getJourneyByIdRoute ~ getJourneyByLabelRoute ~ getAllJourneysRoute

  private lazy val getJourneyByIdRoute: Route = JourneysEndpoint.getJourneyByIdEndpoint.toRoute {
    mapToDeviceJourneyRequest andThen service.getDeviceJourney andThen handleJourneyResult
  }

  private lazy val getJourneyByLabelRoute: Route = JourneysEndpoint.getJourneysByLabelEndpoint.toRoute {
    mapToDeviceJourneysByLabelRequest andThen service.getAllDeviceJourneysByLabel andThen handleTagFeatureVersionResultSeq
  }

  private lazy val getAllJourneysRoute: Route = JourneysEndpoint.getJourneysEndpoint.toRoute {
     mapToDeviceJourneysRequest andThen  service.getAllDeviceJourneys andThen handleTagFeatureVersionResultSeq
  }

  private lazy val handleJourneyResult: Future[Journey] => Future[Either[DomainError, JourneyView]] = future => future.transform {
    case Success(value) => makeSuccessResult(Converters.modelToApi(value))
    case Failure(exception) =>
      log.error(exception.getMessage)
      exception match {
        case e: DomainError => makeFailureResult(e)
        case _ => makeFailureResult(DomainError.UnexpectedServiceError)
      }
  }

  private lazy val handleTagFeatureVersionResultSeq: Future[Seq[Journey]] => Future[Either[DomainError, Seq[JourneyView]]] = future => future.transform {
    case Success(value) => makeSuccessResult(value.map(Converters.modelToApi))
    case Failure(exception) =>
      log.error(exception.getMessage)
      exception match {
        case e: DomainError => makeFailureResult(e)
        case _ => makeFailureResult(DomainError.UnexpectedServiceError)
      }
  }

}

object JourneysApi{
  def apply(service:JourneysService):JourneysApi = new JourneysApi(service)
}

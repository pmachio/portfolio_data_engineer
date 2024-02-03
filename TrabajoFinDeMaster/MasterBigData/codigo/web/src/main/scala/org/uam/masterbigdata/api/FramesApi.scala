package org.uam.masterbigdata.api

import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.api.documentation.{ApiMapper, FramesEndpoint}
import org.uam.masterbigdata.domain.service.FramesService
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.Directives._
import org.uam.masterbigdata.domain.AdapterModel.FrameView
import org.uam.masterbigdata.domain.model.Entities.Frame
import org.uam.masterbigdata.domain.model.error.DomainError
import sttp.tapir.server.akkahttp._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class FramesApi (service:FramesService) extends ApiMapper with ComponentLogging {

  import ApiHelpers._

  lazy val routes: Route = getJourneyByIdRoute ~ getAllJourneysRoute

  private lazy val getJourneyByIdRoute: Route = FramesEndpoint.getFrameByIdEndpoint.toRoute {
    mapToDeviceFrameRequest andThen service.getDeviceFrames andThen handleJourneyResult
  }
  private lazy val getAllJourneysRoute: Route = FramesEndpoint.getFramesEndpoint.toRoute {
    mapToDeviceFramesRequest andThen service.getAllDeviceFrames andThen handleTagFeatureVersionResultSeq
  }

  private lazy val handleJourneyResult: Future[Frame] => Future[Either[DomainError, FrameView]] = future => future.transform {
    case Success(value) => makeSuccessResult(Converters.modelToApi(value))
    case Failure(exception) =>
      log.error(exception.getMessage)
      exception match {
        case e: DomainError => makeFailureResult(e)
        case _ => makeFailureResult(DomainError.UnexpectedServiceError)
      }
  }

  private lazy val handleTagFeatureVersionResultSeq: Future[Seq[Frame]] => Future[Either[DomainError, Seq[FrameView]]] = future => future.transform {
    case Success(value) => makeSuccessResult(value.map(Converters.modelToApi))
    case Failure(exception) =>
      log.error(exception.getMessage)
      exception match {
        case e: DomainError => makeFailureResult(e)
        case _ => makeFailureResult(DomainError.UnexpectedServiceError)
      }
  }
}

object FramesApi{
  def apply(service:FramesService) = new FramesApi(service)
}


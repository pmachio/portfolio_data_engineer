package org.uam.masterbigdata.api.documentation

import org.uam.masterbigdata.api.documentation.ApiEndpoint.{baseEndpoint, framesResource}
import org.uam.masterbigdata.domain.AdapterModel.FrameView
import org.uam.masterbigdata.domain.model.error.DomainError
import sttp.tapir.{Endpoint, EndpointInput, jsonBody, oneOf, path}

trait FramesEndpoint  extends JsonCodecs  with ApiErrorMapping {

  private[api] lazy val frameIdPath = path[Long]("frameId")
  private lazy val frameByIdResource: EndpointInput[(Long, Long)] = framesResource / frameIdPath
  lazy val getFrameByIdEndpoint: Endpoint[(Long, Long), DomainError, FrameView, Nothing] =
    baseEndpoint
      .get
      .name("GetFrameById")
      .description("Recupera un frame por su identificador")
      .in(frameByIdResource)
      .out(jsonBody[FrameView])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))


  lazy val getFramesEndpoint: Endpoint[Long, DomainError, Seq[FrameView], Nothing] =
    baseEndpoint
      .get
      .name("GetAllFrames")
      .description("Recupera todos los frames de un dispositivo")
      .in(framesResource)
      .out(jsonBody[Seq[FrameView]])
      .errorOut(oneOf(statusNotFound, statusConflict, statusBadRequest, statusInternalServerError, statusDefault))

}

object FramesEndpoint extends FramesEndpoint{}

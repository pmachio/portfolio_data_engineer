package org.uam.masterbigdata.api.documentation

import io.circe.syntax._
import io.circe.{Decoder, Encoder, HCursor, Json}
import sttp.model.StatusCode
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.json.circe._
import sttp.tapir.{EndpointOutput, jsonBody, statusDefaultMapping, statusMapping}
import akka.http.scaladsl.model
import org.uam.masterbigdata.domain.model.error.DomainError.{DuplicateEntityError, MessageParsingError, NonExistentEntityError, UnexpectedServiceError}
import org.uam.masterbigdata.domain.model.error.{DomainError, DomainException}

trait ApiErrorMapping {
  // B A D  R E Q U E S T
  private lazy val badRequestDescription = model.StatusCodes.BadRequest.defaultMessage
  private[api] lazy val statusBadRequest: EndpointOutput.StatusMapping[DomainError.MessageParsingError.type] =
    statusMapping(StatusCode.BadRequest, jsonBody[DomainError.MessageParsingError.type].example(MessageParsingError).description(badRequestDescription))

  // C O N F L I C T
  private lazy val conflictDescription = model.StatusCodes.Conflict.defaultMessage
  private[api] lazy val statusConflict: EndpointOutput.StatusMapping[DomainError.DuplicateEntityError.type] =
    statusMapping(StatusCode.Conflict, jsonBody[DomainError.DuplicateEntityError.type].example(DuplicateEntityError).description(conflictDescription))

  // D E F A U L T
  private lazy val defaultDescription = "Unknown Error"
  private[api] lazy val statusDefault: EndpointOutput.StatusMapping[DomainError] =
    statusDefaultMapping(jsonBody[DomainError].example(UnexpectedServiceError).description(defaultDescription))

  // I N T E R N A L  S E R V E R  E R R O R
  private lazy val internalServerErrorDescription = model.StatusCodes.InternalServerError.defaultMessage
  private[api] lazy val statusInternalServerError: EndpointOutput.StatusMapping[DomainError.UnexpectedServiceError.type] =
    statusMapping(StatusCode.InternalServerError, jsonBody[DomainError.UnexpectedServiceError.type].example(UnexpectedServiceError).description(internalServerErrorDescription))

  // N O T  F O U N D
  private lazy val notFoundDescription = model.StatusCodes.NotFound.defaultMessage
  private[api] lazy val statusNotFound: EndpointOutput.StatusMapping[DomainError.NonExistentEntityError.type] =
    statusMapping(StatusCode.NotFound, jsonBody[DomainError.NonExistentEntityError.type].example(NonExistentEntityError).description(notFoundDescription))

  // A S S E T  C O M P O S E R  E R R O R  C O D E C
  private[api] implicit def DomainErrorCodec[A <: DomainError]: JsonCodec[A] =
    implicitly[JsonCodec[Json]].map(json => json.as[A] match {
      case Left(_) => throw DomainException(MessageParsingError)
      case Right(value) => value
    })(error => error.asJson)

  private[api] implicit def encodeDomainError[A <: DomainError]: Encoder[A] =
    (e: A) => Json.obj(
      ("code", Json.fromString(e.code.toString)),
      ("message", Json.fromString(e.message)),
    )

  private[api] implicit def decodeDomainError[A <: DomainError]: Decoder[A] = (c: HCursor) => for {
    output <- c.get[String]("code")
  } yield DomainError.find(output).asInstanceOf[A]
}

object ApiErrorMapping extends ApiErrorMapping

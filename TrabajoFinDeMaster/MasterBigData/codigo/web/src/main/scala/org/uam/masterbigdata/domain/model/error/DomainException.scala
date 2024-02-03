package org.uam.masterbigdata.domain.model.error

import org.uam.masterbigdata.domain.model.error.DomainError._
import org.uam.masterbigdata.domain.model.error.DomainErrorCode.UNEXPECTED

class DomainException (val error: DomainError) extends RuntimeException(error.prettyPrint()) {

  def this(error: String) = this(new DomainError  {
    /**
     * Código asociado al error
     */
    override val code: DomainErrorCode = UNEXPECTED
    /**
     * Descripción del error
     */
    override val message: String = error
  })
}

object DomainException {

  sealed trait HttpClientException

  case class UnknownException(message: String) extends RuntimeException(message)
    with HttpClientException

  case class BadRequestException(message: String) extends RuntimeException(message)
    with HttpClientException

  case class ResourceNotFoundException(message: String) extends RuntimeException(message)
    with HttpClientException

  case class InternalServerErrorException(message: String) extends RuntimeException(message)
    with HttpClientException


  def apply(error: DomainError): DomainException = new DomainException(error)

  def apply(error: String): DomainException = new DomainException(error)

  def unapply(arg: DomainException): Option[DomainError] = Seq(
    DuplicateEntityError,
    NonExistentEntityError,
    MessageParsingError,
    MissingMessageElementError,
    UnexpectedServiceError,
    EmptyRuleSequenceError
  ).find(_.equals(arg.error))

}

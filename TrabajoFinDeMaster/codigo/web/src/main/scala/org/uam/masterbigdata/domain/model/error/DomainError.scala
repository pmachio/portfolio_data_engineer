package org.uam.masterbigdata.domain.model.error

import org.uam.masterbigdata.domain.model.error.DomainErrorCode._

trait DomainError {
  /**
   * Código asociado al error
   */
  val code: DomainErrorCode
  /**
   * Descripción del error
   */
  val message: String


  def prettyPrint(): String = s"DomainError - $code - $message"

}

object DomainError {


  case object DuplicateEntityError extends DomainError {
    override val code: DomainErrorCode = RESOURCE_DUPLICATED
    override val message: String = "An Entity must be unique"
  }

  case object NonExistentEntityError extends DomainError {
    override val code: NON_EXISTENT_RESOURCE.type = NON_EXISTENT_RESOURCE
    override val message: String = "The Entity must exist"
  }

  case object MessageParsingError extends DomainError {
    override val code: RESOURCE_INVALID.type = RESOURCE_INVALID
    override val message: String = "The message has not a valid JSON format"
  }

  case object MissingMessageElementError extends DomainError {
    override val code: DomainErrorCode = NON_EXISTENT_ELEMENT
    override val message: String = "The message does not have an expected element"
  }

  case object UnexpectedServiceError extends DomainError {
    override val code: DomainErrorCode = UNEXPECTED
    override val message: String = "The server encountered an unexpected condition which prevented it from fulfilling the request"
  }


  case object EmptyRuleSequenceError extends DomainError {
    override val code: DomainErrorCode = UNEXPECTED
    override val message: String = "The rule sequence is empty"
  }

  case class UnexpectedInfrastructureError(code: DomainErrorCode = UNEXPECTED, message: String) extends DomainError

  case class ValidationError(msg: String) extends DomainError {
    override val code: DomainErrorCode = UNEXPECTED
    override val message: String = msg
  }

  def prettyPrint(seq: Seq[DomainError]) = seq.mkString("[", ",", "]")

  private val DomainErrors: Set[DomainError] =
    Set(DuplicateEntityError, NonExistentEntityError, MessageParsingError, MissingMessageElementError, UnexpectedServiceError, EmptyRuleSequenceError)


  def find(DomainError: String): DomainError =
    DomainErrors.find(_.toString.equalsIgnoreCase(DomainError)) match {
      case Some(value) => value
      case None => throw DomainException(NonExistentEntityError)
    }
}

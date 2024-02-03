package org.uam.masterbigdata.domain.model.error

sealed trait DomainErrorCode

object DomainErrorCode{
  case object UNEXPECTED extends DomainErrorCode

  case object RESOURCE_INVALID extends DomainErrorCode

  case object RESOURCE_DUPLICATED extends DomainErrorCode

  case object NON_EXISTENT_RESOURCE extends DomainErrorCode

  case object NON_EXISTENT_ELEMENT extends DomainErrorCode
}

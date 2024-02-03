package org.uam.masterbigdata.api

import org.uam.masterbigdata.domain.model.error.DomainError

import scala.util.Success

object ApiHelpers {

    def makeSuccessResult[T](t: T): Success[Right[Nothing, T]] =
      Success(Right(t))

    def makeFailureResult(e: DomainError): Success[Left[DomainError, Nothing]] =
      Success(Left(e))

}

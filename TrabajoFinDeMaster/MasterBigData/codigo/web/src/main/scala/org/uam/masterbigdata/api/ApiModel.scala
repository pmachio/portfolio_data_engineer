package org.uam.masterbigdata.api

object ApiModel {
  case class BuildInfoDto(
                           name: String,
                           version: String,
                         )

  //Errores
  sealed trait OutputError {
    val code: String
    val message: String
  }

  case class BadRequestError(code: String, message: String) extends OutputError

  case class NotFoundError(code: String, message: String) extends OutputError

  case class ConflictError(code: String, message: String) extends OutputError

  case class ServerError(code: String, message: String) extends OutputError
}

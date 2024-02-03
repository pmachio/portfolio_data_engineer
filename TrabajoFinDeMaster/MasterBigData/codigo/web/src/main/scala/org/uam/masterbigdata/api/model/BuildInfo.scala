package org.uam.masterbigdata.api.model

import scala.Predef._
/** Lo suyo seria que se generara con sbt-buildinfo. */
case object BuildInfo{
  val name: String = "webApplication"
  val version: String = "0.0.1"

  val toMap: Map[String, Any] = Map[String, Any](
    "name" -> name,
    "version" -> version)

  val toJson: String = toMap.map { i =>
    def quote(x: Any): String = "\"" + x + "\""

    val key: String = quote(i._1)
    val value: String = i._2 match {
      case elem: Seq[_] => elem.map(quote).mkString("[", ",", "]")
      case elem: Option[_] => elem.map(quote).getOrElse("null")
      case elem => quote(elem)
    }
    s"$key : $value"
  }.mkString("{", ", ", "}")
}

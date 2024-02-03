import sbt._

object Dependencies {

  val production: List[ModuleID] =
    // L O G B A C K
   "net.logstash.logback" % "logstash-logback-encoder" % Version.encoder ::Nil

  val test:List[ModuleID] = "org.scalatest" %% "scalatest" % Version.scalaTest % "test"::Nil

  object Version {
    val akka = "2.6.13"
    val akkaHttp = "10.2.5"
    val akkaKafka = "2.0.7"
    val akkaSlick = "1.1.2"
    val slick_pg = "0.19.0"
    val circe = "0.13.0"
    val json4s = "3.6.11"
    val postgres = "42.2.10"
    val scalaTestVersion = "3.2.15"
    val encoder = "7.2"
    val scalaTest = "3.2.15"
    //local
    //val sparkVersion = "3.2.3"
    //contenedor de spark
    val sparkVersion = "3.1.2"
    val sparkFastTests = "1.3.0"
    val sparkDaria = "1.2.3"
    val tapir = "0.12.21"
  }
}

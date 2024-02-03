import Dependencies.{Version => CommonVersion}
import sbt._

object WebDependencies {
  val production: Seq[ModuleID] = Seq(

    // T A P I R
    "com.softwaremill.sttp.tapir" %% "tapir-core" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-akka-http-server" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-sttp-client" % CommonVersion.tapir,
    "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-akka-http" % CommonVersion.tapir,

    // J S O N  L I B S
    "io.circe" %% "circe-optics" % CommonVersion.circe,
    "io.circe" %% "circe-generic-extras" % CommonVersion.circe,
    "io.circe" %% "circe-shapes" % CommonVersion.circe,

    // A K K A
  "com.typesafe.akka" %% "akka-http" % CommonVersion.akkaHttp,
  "com.typesafe.akka" %% "akka-http-spray-json" % CommonVersion.akkaHttp,
  "com.typesafe.akka" %% "akka-slf4j" % CommonVersion.akka,
  "com.typesafe.akka" %% "akka-stream" % CommonVersion.akka,
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % CommonVersion.akkaSlick
  exclude("com.typesafe", "config")
  exclude("com.typesafe.akka", "akka-actor"),

    // P O S T G R E S
    "org.postgresql" % "postgresql" % CommonVersion.postgres,
    "com.github.tminglei" %% "slick-pg" % CommonVersion.slick_pg,
    "com.github.tminglei" %% "slick-pg_circe-json" % CommonVersion.slick_pg,

    // L O G B A C K
    "net.logstash.logback" % "logstash-logback-encoder" % Version.encoder,
  )


  object Version {
    val encoder = "6.3"
  }
}

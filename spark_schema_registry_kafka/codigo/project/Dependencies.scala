import sbt._

object Dependencies {

  val production: List[ModuleID] =
    // L O G B A C K
   "net.logstash.logback" % "logstash-logback-encoder" % Version.encoder ::Nil


  object Version {
    val encoder = "7.2"
    //local
    //contenedor de spark
    val sparkVersion = "2.4.8"
  }
}

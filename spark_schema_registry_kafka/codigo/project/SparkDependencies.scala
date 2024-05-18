import Dependencies.{Version => CommonVersion}
import sbt._

//Ejecutar spark en local
object SparkDependencies {
  val production: List[ModuleID] =
    "org.apache.spark" %% "spark-core" % CommonVersion.sparkVersion :: // % "provided" ::
      "org.apache.spark" %% "spark-sql" % CommonVersion.sparkVersion :: //% "provided" ::
      // streaming-kafka
      "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % CommonVersion.sparkVersion ::
  //repositorio de cloudera
      "com.hortonworks" % "spark-schema-registry" % "1.1.0.7.2.17.0-334"::
       Nil


}




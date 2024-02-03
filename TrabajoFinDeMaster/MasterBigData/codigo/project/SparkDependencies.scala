import Dependencies.{Version => CommonVersion}
import sbt._

//Ejecutar spark en local
object SparkDependencies {
  val production: List[ModuleID] =
    "org.apache.spark" %% "spark-core" % CommonVersion.sparkVersion :: // % "provided" ::
      "org.apache.spark" %% "spark-sql" % CommonVersion.sparkVersion :: //% "provided" ::
      "org.apache.spark" %% "spark-mllib" % CommonVersion.sparkVersion :: //% "provided" ::
      // streaming-kafka
      "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % CommonVersion.sparkVersion ::
      "org.postgresql" % "postgresql" % CommonVersion.postgres :: Nil

  //https://github.com/MrPowers/spark-fast-tests
  ////https://github.com/MrPowers/spark-daria
  val test: List[ModuleID] = "org.scalatest" %% "scalatest" % CommonVersion.scalaTest % "test" ::
    "com.github.mrpowers" %% "spark-fast-tests" % CommonVersion.sparkFastTests % "test" ::
    "com.github.mrpowers" %% "spark-daria" % CommonVersion.sparkDaria % "test" :: Nil
}




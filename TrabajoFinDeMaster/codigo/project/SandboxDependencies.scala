import Dependencies.{Version => CommonVersion}
import sbt._

object SandboxDependencies {
  val production:List[ModuleID] =  "org.apache.spark" %% "spark-sql" % CommonVersion.sparkVersion % "provided":: Nil
  val test:List[ModuleID] = "org.scalatest" %% "scalatest" % CommonVersion.scalaTest % "test"::
    //https://github.com/MrPowers/spark-fast-tests
    "com.github.mrpowers" %% "spark-fast-tests" % CommonVersion.sparkFastTests % "test" ::
  ////https://github.com/MrPowers/spark-daria
   "com.github.mrpowers" %% "spark-daria" % CommonVersion.sparkDaria::Nil
}

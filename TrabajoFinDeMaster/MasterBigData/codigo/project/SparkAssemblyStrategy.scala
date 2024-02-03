import sbt.Def
import sbt.Keys.mainClass
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.assemblyJarName
import sbtassembly.MergeStrategy
object SparkAssemblyStrategy {
  val value: Seq[Def.Setting[_]] = Seq(
    assembly / assemblyJarName := "spark_project.jar",
    //como la parte de Stream y Batch tienen cada una su clase principal, no indicamos cual es la del emsamblado
    //assembly / mainClass := Some("org.uam.masterbigdata.WebApplication"),

    assembly / assemblyMergeStrategy := {
      case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
}

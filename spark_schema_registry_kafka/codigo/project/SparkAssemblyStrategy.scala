import sbt.Def
import sbt.Keys.mainClass
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.assemblyJarName
import sbtassembly.MergeStrategy
object SparkAssemblyStrategy {
  val value: Seq[Def.Setting[_]] = Seq(
    assembly / assemblyJarName := "spark_project.jar",
    assembly / assemblyMergeStrategy := {
      case "org/apache/spark/unused/UnusedStubClass.class" => MergeStrategy.last
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
}

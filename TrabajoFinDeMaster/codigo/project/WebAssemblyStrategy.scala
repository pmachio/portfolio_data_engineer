import sbt.Def
import sbt.Keys.mainClass
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin.autoImport.assemblyJarName
import sbtassembly.MergeStrategy

object WebAssemblyStrategy {
  val value: Seq[Def.Setting[_]] = Seq(
    assembly / assemblyJarName := "web.jar",
    assembly / mainClass := Some("org.uam.masterbigdata.WebApplication"),
    assembly / assemblyMergeStrategy  := {
      case "module-info.class" => MergeStrategy.last
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )
}

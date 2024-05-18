import Dependencies.{Version => CommonVersion}
import sbt._

//Para ejecutar Spark en un cluster
object SparkDependenciesProvided {
  val production: List[ModuleID] =
    "org.apache.spark" %% "spark-core" % CommonVersion.sparkVersion %  "provided" ::
      "org.apache.spark" %% "spark-sql" % CommonVersion.sparkVersion % "provided" ::
      // streaming-kafka. Esta depedencia se hace explicita cuando se envia al cluster con submit con con la opcion packages.
       //la version debe coincidir con la del spark que tiene el cluster. https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#deploying
      // spark-submit --/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2
      "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % CommonVersion.sparkVersion ::
  //repositorio de cloudera
      "com.hortonworks" % "spark-schema-registry" % "1.1.0.7.2.17.0-334"::
      Nil

}

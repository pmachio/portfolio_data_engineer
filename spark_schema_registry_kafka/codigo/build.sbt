ThisBuild / scalaVersion := "2.11.8"
import Dependencies.{Version => CommonVersion}



lazy val spark_proj = (project in file("spark_proj"))
  .settings(name := "spark_proj")
  .settings(SparkAssemblyStrategy.value)
  .settings(resolvers ++= Seq("Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"))
  //si queremos crear el ensamblado para lanzar spark en un cluster debemos usar SparkDependenciesProvided, para local SparkDependencies
  //.settings(libraryDependencies ++= SparkDependenciesProvided.production)
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % CommonVersion.sparkVersion ,
      "org.apache.spark" %% "spark-sql" % CommonVersion.sparkVersion exclude("org.slf4j", "slf4j-log4j12") exclude("org.eclipse.jetty", "jetty-servlet") ,
      // streaming-kafka
      "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % CommonVersion.sparkVersion
      //repositorio de cloudera. Basado en el pom https://repository.cloudera.com/repository/libs-release-local/com/hortonworks/spark-schema-registry/1.1.0.7.1.8.11-3/spark-schema-registry-1.1.0.7.1.8.11-3.pom
/*
    ,"com.hortonworks" % "spark-schema-registry" % "1.1.0.7.1.8.11-3",
"com.hortonworks.registries" % "schema-registry" % "0.5.2",
"com.hortonworks.registries" % "schema-registry-webservice" % "0.5.2" exclude("org.slf4j", "log4j-over-slf4j")  exclude("org.slf4j", "slf4j-log4j12")
  exclude("log4j", "log4j") exclude("com.hortonworks.registries", "registry-common") exclude("com.hortonworks.registries", "registry-webservice")
  exclude("com.hortonworks.registries", "schema-registry-core")*/
)

)











ThisBuild / scalaVersion := "2.11.12"



lazy val spark_proj = (project in file("spark_proj"))
  .settings(name := "spark_proj")
  .settings(SparkAssemblyStrategy.value)
  .settings(resolvers ++= Seq("Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"))
  //si queremos crear el ensamblado para lanzar spark en un cluster debemos usar SparkDependenciesProvided, para local SparkDependencies
  //.settings(libraryDependencies ++= SparkDependenciesProvided.production)
  .settings(libraryDependencies ++= SparkDependencies.production)











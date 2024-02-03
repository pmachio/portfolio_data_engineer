ThisBuild / scalaVersion := "2.12.12"

lazy val spark_proj = (project in file("spark_proj"))
  .settings(name := "spark_proj")
  .settings(SparkAssemblyStrategy.value)
  //si queremos crear el ensamblado para lanzar spark en un cluster debemos usar SparkDependenciesProvided, para local SparkDependencies
  .settings(libraryDependencies ++= SparkDependenciesProvided.production)
  .settings(libraryDependencies ++= SparkDependenciesProvided.test)


lazy val web = (project in file("web"))
  .settings(name := "web")
  .settings(WebAssemblyStrategy.value)
  .settings(libraryDependencies ++= WebDependencies.production)


//Para trastear
lazy val sandbox = (project in file("sandbox"))
  .settings(name := "sandbox")
  .settings(libraryDependencies ++= SandboxDependencies.production)
  .settings(libraryDependencies ++= SandboxDependencies.test)




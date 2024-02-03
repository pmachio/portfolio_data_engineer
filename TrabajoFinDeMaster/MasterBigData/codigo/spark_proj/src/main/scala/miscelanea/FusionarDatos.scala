package miscelanea

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.uam.masterbigdata.Schemas

object FusionarDatos extends App with Schemas{

    val spark = SparkSession.builder()
      .appName("ColumnsAndExpressionsExercises")
      .config("spark.master", "local[*]")
      .getOrCreate()

    spark.read
      .option("inferSchema", "true")
      .json("/Users/machio/Dropbox/MasterBigData/TrabajoFinDeMaster/entorno/data/datos.2.json").printSchema()

    val datos1 = spark.read
      .schema(telemetry_schema)
      .json("/Users/machio/Dropbox/MasterBigData/TrabajoFinDeMaster/entorno/data/datos.json")

    val datos2 = spark.read
      .schema(telemetry_schema)
      .json("/Users/machio/Dropbox/MasterBigData/TrabajoFinDeMaster/entorno/data/datos.2.json")

    datos1.union(datos2).write
      .mode(SaveMode.Overwrite)
      .save("batch_layer/src/main/resources/data/datos.parquet")
}

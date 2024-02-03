package org.uam.masterbigdata.drivers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.uam.masterbigdata.{JourneysHelper, Schemas}

/**
 * Usado para hacer pruebas en local
 * Recuerda en BatchDependecies quitar de las las dependecias de spark  el provided
 * */
object LocalDriver extends Schemas {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("Need 1) json path")
      System.exit(1)
    }
    println(s"Json path ${args(0)}")
    val spark = SparkSession.builder()
      //quitar para el submit
      .master("local[*]")
      .appName("ParseJourneys")
      .getOrCreate()

    /** Por parámetros le pasamos la dirección del archivo */
    //Lee el archivo,
    //val telemetryDF: DataFrame = spark.read.option("mode", "FAILFAST").schema(telemetry_schema).json(args(0))
    val telemetryDF: DataFrame = spark.read.option("mode", "FAILFAST").schema(telemetry_schema).json("/Users/machio/Dropbox/MasterBigData/TrabajoFinDeMaster/entorno/data/datos_1328414834680696832.json")

     /**Trayectos*/
     //convierte a trayecto
     val journeysDF:DataFrame = telemetryDF.transform(JourneysHelper.calculateJourneys())
    journeysDF.show()

     journeysDF.where(col("start_timestamp").isNull).show()
     journeysDF.where(col("start_location_address").isNull).show()
    /*
        /**Eventos*/
         val eventsDF: DataFrame = telemetryDF.transform(EventsHelper.createExcessiveThrottleEvent())
         //Muestra los 3 primeros
         eventsDF.show(3)
         //Escribe en base de datos
         saveDataFrameInPostgresSQL(eventsDF, "events")
    */
  }
}

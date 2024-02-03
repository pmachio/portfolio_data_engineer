package org.uam.masterbigdata.drivers

import org.apache.spark.sql.{ DataFrame, SparkSession}
import org.uam.masterbigdata.{EventsHelper, JourneysHelper, Schemas}

/**
 * Usado para enviar al cluster de Spark
 * Recuerda en BatchDependecies establecer las depencias de spark como provided
 * */
object ToSubmitDriver extends Schemas with DatabaseWriter {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println(
        """Se necesita
          | 1) path del archivos JSON
          | 2) path del modelo de ML para clasificar los trayectos""".stripMargin)
      System.exit(1)
    }
    println(s"path del archivos JSON ${args(0)}")
    println(s"path del modelo de clasificación ${args(1)}")

    val spark = SparkSession.builder()
      .appName("ParseJourneys")
      .getOrCreate()

    /** Por parámetros le pasamos la dirección del archivo */
    //Lee el archivo,
    val telemetryDF:DataFrame = spark.read.option("mode", "FAILFAST").schema(telemetry_schema).json(args(0))

    /**Trayectos*/
    //convierte a trayecto
    val journeysDF:DataFrame = telemetryDF.transform(JourneysHelper.calculateLabeledJourneys(args(1)))
    //Depuración. Muestra los 3 primeros
    //journeysDF.show(3)

    //Escribe en base de datos
    saveDataFrameInPostgresSQL(journeysDF, "journeys")

   /**Eventos*/
    val eventsDF: DataFrame = telemetryDF.transform(EventsHelper.createExcessiveThrottleEvent())
    //Depuración. Muestra los 3 primeros
    //eventsDF.show(3)
    //Escribe en base de datos
    saveDataFrameInPostgresSQL(eventsDF, "events")

  }
}

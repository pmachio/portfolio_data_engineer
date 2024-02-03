package org.uam.masterbigdata.drivers

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.StringType
import org.uam.masterbigdata.{EventsHelper, Schemas}

import java.sql.Timestamp

object StreamDriver extends Schemas with DatabaseWriter {
  case class Event(id: String, device_id: Long, created: Timestamp, type_id: Long, location_address: String, location_latitude: Double, location_longitude: Double, value: String)

  val spark: SparkSession = SparkSession
    .builder()
    //quitar al hacer submit al cluster
    //.master("local[*]")
    .appName("Streaming")
    .getOrCreate()

  private def createStreamEvents(): Unit = {
    //si queremos usar el modo depuración usamos 'readFromSocket' en lugar de 'loadKafkaStream'
    val streamDF: DataFrame = loadKafkaStream()

    val eventsDF = streamDF.select(from_json(col("value"), telemetry_schema).as("json"))
      .selectExpr("json.*")
      .transform(EventsHelper.createFuelStealingEvent())

    //si queremos usar el modo depuración usamos 'writeEventStreamIntoConsole' en lugar de 'writeEventStreamIntoPostgres'
    val query: StreamingQuery = writeEventStreamIntoPostgres(eventsDF)
    query.awaitTermination()

  }


  private def loadKafkaStream(): DataFrame = spark.readStream
    .format("kafka")
    .options(
      Map(
        //Para pruebas eb local usar localhost en lugar de kafka como direccion del bootstrap server. El nombre corresponde al servicio de docker-compose
        "kafka.bootstrap.servers" -> "kafka:9092"
        , "subscribe" -> "telemetry"
      )
    ).load()
    .select(col("value").cast(StringType).as("value"))

  /**Con fines de depuración. Usar para probar stream mediante socket, por ejemplo con netcat (nc -lk 12345) */
  private def readFromSocket(): DataFrame = spark.readStream
    .format("socket")
    .options(
      Map(
        "host" -> "127.0.0.1"
        , "port" -> "12345"
      )
    )
    .load()
/**Con fines de depuración. Leer de consola los resultados del stream*/
  private def writeEventStreamIntoConsole(eventDF:DataFrame) = {
      eventDF
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .start()
  }
  private def writeEventStreamIntoPostgres(eventDF: DataFrame): StreamingQuery = {
    eventDF.as[Event](Encoders.product[Event])
      .writeStream.foreachBatch(
      (batch: Dataset[Event], _: Long) => batch.write
        .format("jdbc")
        .mode(SaveMode.Append)
        .options(
          props
        )
        .option("dbtable", s"public.Events")
        .save()
    )
      .start()

  }

  def main(args: Array[String]): Unit = {
    createStreamEvents()
  }
}

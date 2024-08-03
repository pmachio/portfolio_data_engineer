package org.pmachio.playground

//import com.hortonworks.spark.registry.util._
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

import java.util.UUID

object PlayingAround {
def main(args:Array[String]):Unit = {
  val schemaRegistryUrl =  "http://localhost:9090/api/v1/"
  val bootstrapServers =  "localhost:9092"
  val topic = "topic1"
  val schemaName = "users"
  val outTopic = "topic1-out"
  val checkpointLocation = "/Users/machio/Documents/temp_checkpoint"
  val securityProtocol:Option[String] =  None


  val spark = SparkSession.builder()
    .appName("SchemaRegistryExample")
    .master("local[2]")
    .getOrCreate()
  /*
  val reader =  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", topic)
    .load()




  val mess:DataFrame = securityProtocol
    .map(p => reader.option("kafka.security.protocol", p).load())
    .getOrElse(reader.load())

  import spark.implicits._

  // the schema registry client config
  val config = Map[String, Object]("schema.registry.url" -> schemaRegistryUrl)

  // the schema registry config that will be implicitly passed
  implicit val srConfig: SchemaRegistryConfig = SchemaRegistryConfig(config)

  // Read messages from kafka and deserialize.
  // This uses the schema registry schema
  // associated with the topic and maps it to spark schema.
  //val df = reader
    //.select(from_sr(col("value"), schemaName).alias("message"))
  //query.awaitTermination()
  */


  val kafkaDF: DataFrame =   spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)
    .load()

  val query = kafkaDF
    .select(col("topic"), expr("cast(value as string) as actualValue"))
    .writeStream
    .format("console")
    /*.format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("topic", outTopic)
    .option("checkpointLocation", checkpointLocation)
    .trigger(Trigger.ProcessingTime(10000))*/
    .outputMode(OutputMode.Append())
    .start()


  query.awaitTermination()
}
}

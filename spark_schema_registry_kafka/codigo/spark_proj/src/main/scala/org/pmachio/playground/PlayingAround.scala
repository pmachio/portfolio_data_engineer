package org.pmachio.playground

import com.hortonworks.spark.registry.util._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import java.util.UUID

object PlayingAround {
  val schemaRegistryUrl =  "http://localhost:9090/api/v1/"
  val bootstrapServers =  "localhost:9092"
  val topic = "topic1"
  val outTopic = "topic1-out"
  val checkpointLocation = "/tmp/temporary-" + UUID.randomUUID.toString
  val securityProtocol:Option[String] =  None

  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("SchemaRegistryAvroExample")
    .getOrCreate()

  val reader = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)



  val messages:DataFrame = securityProtocol
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
  val df = messages
    .select(from_sr($"value", topic).alias("message"))

  val query = df.writeStream
    .format("console")
    .outputMode(OutputMode.Append())
    .start()

  query.awaitTermination()

  def main(args:Array[String]):Unit = {

  }
}

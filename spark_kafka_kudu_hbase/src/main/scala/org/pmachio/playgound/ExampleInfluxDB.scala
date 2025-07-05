package org.pmachio.playgound

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}


object ExampleInfluxDB {
  def main(args: Array[String]): Unit = {

    //Define spark session
    val sparkConf = new SparkConf()
      .setAppName("Spark HBase Example")
      .setMaster("local[*]")

    //Influx db
    sparkConf.set("spark.metrics.conf.*.sink.influx.class","org.apache.spark.metrics.sink.InfluxDbSink")
    sparkConf.set("spark.metrics.conf.*.sink.influx.protocol","http")
    sparkConf.set("spark.metrics.conf.*.sink.influx.host","localhost")
    sparkConf.set("spark.metrics.conf.*.sink.influx.port","8086")
    sparkConf.set("spark.metrics.conf.*.sink.influx.period", "10")
    sparkConf.set("spark.metrics.conf.*.sink.influx.unit","seconds")
    //sparkConf.set("spark.metrics.conf.*.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
    sparkConf.set("spark.metrics.staticSources.enabled", "true")
    sparkConf.set("spark.metrics.appStatusSource.enabled","true")
    sparkConf.set("spark.sql.streaming.metricsEnabled","true")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()


    val streamDF:DataFrame = spark.readStream
      .format("socket")
      .options(
        Map(
         "host"->"localhost",
         "port"->"12345"
        )
      ).load()

 val query:StreamingQuery = streamDF.writeStream
   .format("console")
   .outputMode(OutputMode.Append())
   .start()

 query.awaitTermination()

  }
}

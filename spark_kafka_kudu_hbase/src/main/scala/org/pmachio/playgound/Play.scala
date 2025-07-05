package org.pmachio.playgound

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Play {
  def main(args: Array[String]): Unit = {

    //Define spark session
    val sparkConf = new SparkConf()
      .setAppName("Spark HBase Example")
      .setMaster("local[*]")
      /**Spark Dashboards Example*/
      .set("spark.metrics.conf.*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink")
      .set("spark.metrics.conf.*.sink.graphite.host", "dashboard")
      .set("spark.metrics.conf.*.sink.graphite.port", "2003")
      .set("spark.metrics.conf.*.sink.graphite.period", "10")
      .set("spark.metrics.conf.*.sink.graphite.unit","seconds")
      .set("spark.metrics.conf.*.sink.graphite.prefix","spark")
      .set("spark.metrics.conf.*.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
      .set("spark.metrics.staticSources.enabled","true")
      .set("spark.metrics.appStatusSource.enabled","true")
      .set("spark.metrics.appStatusSource.enabled","true")
      .set("spark.sql.streaming.metricsEnabled","true")


    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Hbase connection setup with help of zookeeper:
    val conf =  HBaseConfiguration.create()
    //conf.set("hbase.zookeeper.quorum", "hbase-docker") //zookeeper servers. En este caso la imagen de habse tiene embebido un zookeeper. Hay que meter el nombre del archivo en el fichero Hosts
    conf.set("hbase.zookeeper.quorum", "localhost") //zookeeper servers. En este caso la imagen de habse tiene embebido un zookeeper. Hay que meter el nombre del archivo en el fichero Hosts
    conf.set("hbase.zookeeper.property.clientPort", "2181") // zookeeper server port
    new HBaseContext(spark.sparkContext, conf)



    //Read data from Hbase into a Spark Dataframe:

    val sql = spark.sqlContext

    val hbaseTable = "default:WAR_PLAN" // Hbase table name. namespace default

    val columnMapping =
      """id string :key,
        |infantryNumber string infantry:number,
        |cavalryNumber string cavalry:number""".stripMargin //Mapping between Hbase table and Spark dataframe

    val hbaseSource = "org.apache.hadoop.hbase.spark" // Library responsible for fetching hbase data into spark

    val hbaseData = sql.read.format(hbaseSource).option("hbase.columns.mapping", columnMapping).option("hbase.table", hbaseTable)
    val hbaseDf= hbaseData.load() //Load data into a Dataframe
    hbaseDf.createOrReplaceTempView("hbaseDataframe") // Save the dataframe into a temp view for sql querying


    hbaseDf.show() // Check dataframe content
    hbaseDf.printSchema // Check dataframe mapped columns

    //Write data from Spark dataframe into an hbase table :

    //val hiveTmp = spark.sql("select * from default.war_plan") // Select data from Hive table

    //val columns: Array[String]= hbaseDf.columns
    //val hiveDf = hiveTmp.select(columns.head, columns.tail: _*) // Select hive dataframe columns in the same order as those for hbase.
   // hiveDf.createOrReplaceTempView("hiveDataframe")


    //val insertStatement = "insert into hiveDataframe select * from hbaseDataframe"
    //spark.sql(insertStatement) // Execute the insert statement.




  }
}

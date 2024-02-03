package org.uam.masterbigdata

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.funspec.AnyFunSpec


class ClassifierHelperSpec extends AnyFunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper
  with DataFrameTestHelper
  with Schemas {

  val originalDataFrame: DataFrame = jsonToDF(List(
    s"""{"id":"Tres Cantos - Moralzarzal 1", "device_id":1328414834680696832
       |,"start_timestamp":"2023-02-05 00:00:01" ,"start_location_address":""
       |,"start_location_latitude":40.614105 ,"start_location_longitude":-3.711923
       |,"end_timestamp":"2023-02-05 00:00:02" ,"end_location_address":""
       |,"end_location_latitude":40.6804, "end_location_longitude":-3.976797
       |,"distance":0, "consumption":0 ,"label":"Tres Cantos - Moralzarzal"}""".stripMargin
    ,
    s"""{"id":"Moralzarzal - Tres Cantos 1", "device_id":1328414834680696832
       |,"start_timestamp":"2023-02-05 00:00:03" ,"start_location_address":""
       |,"start_location_latitude":40.6804 ,"start_location_longitude":-3.976797
       |,"end_timestamp":"2023-02-05 00:00:04" ,"end_location_address":""
       |,"end_location_latitude":40.614105, "end_location_longitude":-3.711923
       |,"distance":0, "consumption":0 ,"label":"Moralzarzal - Tres Cantos"}""".stripMargin
    ,
    s"""{"id":"Tres Cantos - Arganzuela 1", "device_id":1328414834680696832
       |,"start_timestamp":"2023-02-05 00:00:04" ,"start_location_address":""
       |,"start_location_latitude":40.614334 ,"start_location_longitude":-3.723002
       |,"end_timestamp":"2023-02-05 00:00:05" ,"end_location_address":""
       |,"end_location_latitude":40.38668, "end_location_longitude":-3.688833
       |,"distance":0, "consumption":0 ,"label":"Tres Cantos - Arganzuela"}""".stripMargin
    ,
    s"""{"id":"Arganzuela - Tres Cantos 1", "device_id":1328414834680696832
       |,"start_timestamp":"2023-02-05 00:00:05" ,"start_location_address":""
       |,"start_location_latitude":40.38668 ,"start_location_longitude":-3.688833
       |,"end_timestamp":"2023-02-05 00:00:06" ,"end_location_address":""
       |,"end_location_latitude":40.614334, "end_location_longitude":-3.723002
       |,"distance":0, "consumption":0 ,"label":"Arganzuela - Tres Cantos"}""".stripMargin
    ,
    s"""{"id":"TC1 - TC2 - 1 ", "device_id":1328414834680696832
       |,"start_timestamp":"2023-02-05 00:00:06" ,"start_location_address":""
       |,"start_location_latitude":40.614448  ,"start_location_longitude":-3.722875
       |,"end_timestamp":"2023-02-05 00:00:07" ,"end_location_address":""
       |,"end_location_latitude":40.600124, "end_location_longitude":-3.700752
       |,"distance":0, "consumption":0 ,"label":"TC1 - TC2"}""".stripMargin
    ,
    s"""{"id":"TC2 - TC1 - 1 ", "device_id":1328414834680696832
       |,"start_timestamp":"2023-02-05 00:00:07" ,"start_location_address":""
       |,"start_location_latitude":40.600124 ,"start_location_longitude":-3.700752
       |,"end_timestamp":"2023-02-05 00:00:08" ,"end_location_address":""
       |,"end_location_latitude":40.614448, "end_location_longitude":-3.722875
       |,"distance":0, "consumption":0 ,"label":"TC2 - TC1"}""".stripMargin)
    , journey_schema)

  describe("generateData") {
    it("Generates new rows for a DataFrames of Journeys with small changes (111.1m) in the latitude and longitude fields") {
      val extendedDataFrame = ClassifierHelper.generateData(originalDataFrame)

     println(extendedDataFrame.count())

      val labelTC2_TC1_distribution = extendedDataFrame
        .where(col("label") === "TC2 - TC1")
        .select("start_location_latitude", "start_location_longitude","end_location_latitude", "end_location_longitude" )
        .summary("mean", "max", "min", "stddev")

      //comprobamos que la desviación estandar no es mayor a 0.001, los coordenadas se mantienen entorno a los 111.1 metros para una etiqueta
      val stddev_labelTC2_TC1 = labelTC2_TC1_distribution.collectAsList().get(3)
      assert(0.001 > stddev_labelTC2_TC1.get(1).asInstanceOf[String].toDouble)
      assert(0.001 > stddev_labelTC2_TC1.get(2).asInstanceOf[String].toDouble)
      assert(0.001 > stddev_labelTC2_TC1.get(3).asInstanceOf[String].toDouble)
      assert(0.001 > stddev_labelTC2_TC1.get(4).asInstanceOf[String].toDouble)

      //Si queremos ver las estadísticas quitar el comentario de  la siguiente linea
      //labelTC2_TC1_distribution.show()

      val labelTC1_TC2_distribution = extendedDataFrame
        .where(col("label") === "TC1 - TC2")
        .select("start_location_latitude", "start_location_longitude","end_location_latitude", "end_location_longitude" )
        .summary("mean", "max", "min", "stddev")

      val stddev_labelTC1_TC2 = labelTC1_TC2_distribution.collectAsList().get(3)
      assert(0.001 > stddev_labelTC1_TC2.get(1).asInstanceOf[String].toDouble)
      assert(0.001 > stddev_labelTC1_TC2.get(2).asInstanceOf[String].toDouble)
      assert(0.001 > stddev_labelTC1_TC2.get(3).asInstanceOf[String].toDouble)
      assert(0.001 > stddev_labelTC2_TC1.get(4).asInstanceOf[String].toDouble)
      //Si queremos ver las estadísticas quitar el comentario de  la siguiente linea
        //labelTC1_TC2_distribution.show()
    }
  }

  describe("journeysClassification_LogReg"){
    it("train"){
      val extendedDataFrame = ClassifierHelper.generateData(originalDataFrame)
      ClassifierHelper.journeysClassification_LogReg("../entorno/data/journeys_logreg_cv", extendedDataFrame)
    }
  }

}

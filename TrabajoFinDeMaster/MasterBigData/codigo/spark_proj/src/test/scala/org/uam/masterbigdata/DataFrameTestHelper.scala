package org.uam.masterbigdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

trait DataFrameTestHelper extends SparkSessionTestWrapper {
  /**
   * pasa una cadena de texto en formato json a dataframe siguiendo el esquema indicado
   * */
  def jsonToDF(json: List[String], schema: StructType): DataFrame =
    spark.createDF(
      json
      , List(StructField("raw_json", StringType))
    ).select(from_json(col("raw_json"), schema).as("formatted_json"))
      .selectExpr("formatted_json.*")
}

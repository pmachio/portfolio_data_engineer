package org.uam.masterbigdata

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, when}

object DateHelper {
  def convertToDate(inputColName: String, outputColName: String)(df: DataFrame) =
    df.withColumn(outputColName
      , when(col(inputColName).contains("."), to_timestamp(col(inputColName)
        , "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      ).otherwise(to_timestamp(col(inputColName), "yyyy-MM-dd'T'HH:mm:ss'Z'")))
}

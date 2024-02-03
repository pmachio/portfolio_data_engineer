package org.uam.masterbigdata

import org.apache.spark.ml.feature.{IndexToString, StringIndexerModel}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, explode, expr, first, array_max, lag, last, lit, round, sum, when}



object JourneysHelper {
  def calculateLabeledJourneys(path: String)(df: DataFrame): DataFrame = {
    val journeysDF: DataFrame = df.transform(calculateJourneys())

    val model = PipelineModel.load(path)
    val tempDF = model.transform(journeysDF)
    //para fines de depuración
    //tempDF.show()

    //recupera las relación de las etiquetas y su indices para poder reetiquetar
    val stringIndexerModel: StringIndexerModel = model.stages
      .filter(_.isInstanceOf[StringIndexerModel])
      .map(_.asInstanceOf[StringIndexerModel]).apply(0)

    val converter: IndexToString = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("label_original")
      .setLabels(stringIndexerModel.labelsArray(0))

    //convertimos los indices de las etiquetas a texto y nos quedamos solo con los campos de journeys.
    //si no hay etiqueta alguna que supere la probabilidad de 0.9 entonces establecemos la etiqueta como unknown
    converter
      .transform(tempDF)
      .select(col("id"), col("device_id"), col("start_timestamp"), col("start_location_address")
      ,col("start_location_latitude"), col("start_location_longitude"), col("end_timestamp")
      ,col("end_location_address"), col("end_location_latitude"), col("end_location_longitude")
      ,col("distance"), col("consumption")
       , when(
          array_max(vector_to_array(col("probability") , "float64")) >= lit(0.90)
          , col("label_original")
        )
          .otherwise(lit("unknown")).as("label_original")
      )
      .withColumnRenamed("label_original", "label")
  }


  def calculateJourneys()(df: DataFrame): DataFrame = {
    df.where(col("gnss").getField("coordinate").isNotNull)
      .select(
        col("attributes")
        , col("timestamp")
        , col("gnss")
        , col("can")
        , col("ignition")
      )
      .transform(flatMainFields())
      .transform(setIgnitionStateChange())
      .where(col("ignition") =!= false)
      .transform(setGroupOfStateChangesToFrames())
      .transform(setInitialStateChangeValues())
      .transform(setFinalStateChangeValues())
      .transform(setCountersValues())
      .transform(aggregateStateChangeValues())
  }

  def flatMainFields()(df: DataFrame): DataFrame = {
    df.transform(CommonTelemetryHelper.flatBasicFields())
      .withColumn("ignition", col("ignition").getField("status"))
  }

  private val window_partition_by_deviceId_order_by_timestamp = Window
    .partitionBy(col("device_id"))
    .orderBy(col("timestamp"))
  private val window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound = Window
    .partitionBy(col("device_id"), col("state_changed_group"))
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    .orderBy(col("timestamp"))
  private val window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp = Window
    .partitionBy(col("device_id"), col("state_changed_group"))
    .orderBy(col("timestamp"))


  /** Creates a new column, state_changed, which marks the state change of column ignition compare with the previous one.
   * If there is a change the value is 1 if not the value is 0 */
  def setIgnitionStateChange()(df: DataFrame): DataFrame = {
    df.withColumn("state_changed"
      , when(col("ignition") === coalesce(lag(col("ignition"), 1).over(window_partition_by_deviceId_order_by_timestamp), col("ignition")), 0)
        .when(col("ignition") =!= coalesce(lag(col("ignition"), 1).over(window_partition_by_deviceId_order_by_timestamp), col("ignition")), 1)
    )
  }

  /** Creates a new column, state_changed_group, with a incremental identifier base on column state_changed.
   * It sum the value of column state_changed, so when there is a state change the identifier is increased by 1 */
  def setGroupOfStateChangesToFrames()(df: DataFrame): DataFrame = {
    df.withColumn("state_changed_group"
      , sum(col("state_changed")).over(window_partition_by_deviceId_order_by_timestamp)
    )
  }

  /**
   * Sets all the initial values, like the timestamp, latitude, longitude, address
   * */
  def setInitialStateChangeValues()(df: DataFrame): DataFrame = {
    df.withColumn("start_timestamp"
      , first(col("timestamp")).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
    )
      .withColumn("start_location_address"
        , first(col("location_address")).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
      .withColumn("start_location_latitude"
        , first(col("location_latitude")).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
      .withColumn("start_location_longitude"
        , first(col("location_longitude")).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
  }

  /**
   * Sets all the final values, like the timestamp, latitude, longitude, address
   * */
  def setFinalStateChangeValues()(df: DataFrame): DataFrame = {
    df.withColumn("end_timestamp"
      , last(col("timestamp"), true).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
    )
      .withColumn("end_location_address"
        , last(col("location_address"), true).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
      .withColumn("end_location_latitude"
        , last(col("location_latitude"), true).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
      .withColumn("end_location_longitude"
        , last(col("location_longitude"), true).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp_unbound)
      )
  }

  /**
   * Calculate the counters values
   * Distance : difference between the current mileage distance and the previous one. In meters
   * Consumption : difference between the current fuel consumed volume and the previous one. In liters
   * */
  def setCountersValues()(df: DataFrame): DataFrame = {
    val canbusDistanceCol: Column = col("can").getField("vehicle").getField("mileage").getField("distance")
    val canbusDistanceColLag: Column = lag(canbusDistanceCol, 1).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp)
    val canbusDistanceColValue: Column = canbusDistanceCol - coalesce(canbusDistanceColLag, canbusDistanceCol)

    val canbusConsumptionCol: Column = col("can").getField("fuel").getField("consumed").getField("volume")
    val canbusConsumptionColLag: Column = lag(canbusConsumptionCol, 1).over(window_partition_by_deviceId_and_by_stateChangedGroup_order_by_timestamp)
    val canbusConsumptionValue: Column = canbusConsumptionCol - coalesce(canbusConsumptionColLag, canbusConsumptionCol)

    df.withColumn("distance"
      , when(canbusDistanceCol.isNotNull
        , canbusDistanceColValue)
        .otherwise(0)
    ).withColumn("consumption"
      , when(canbusConsumptionCol.isNotNull
        , canbusConsumptionValue)
        .otherwise(0)
    )
  }

  /**
   * Aggregates the counters value by device, state_changed_group, start_* and end_value:
   * * Sum all the consumption
   * * Sum all the distance
   * */
  def aggregateStateChangeValues()(df: DataFrame): DataFrame = {
    df.select(
      col("state_changed_group")
      , col("device_id")
      , col("start_timestamp")
      , col("start_location_address")
      , col("start_location_latitude")
      , col("start_location_longitude")
      , col("end_timestamp")
      , col("end_location_address")
      , col("end_location_latitude")
      , col("end_location_longitude")
      , col("distance")
      , col("consumption")
    ).groupBy(
      col("state_changed_group")
      , col("device_id")
      , col("start_timestamp")
      , col("start_location_address")
      , col("start_location_latitude")
      , col("start_location_longitude")
      , col("end_timestamp")
      , col("end_location_address")
      , col("end_location_latitude")
      , col("end_location_longitude")
    ).agg(
      sum(col("distance")).as("distance")
      , sum(col("consumption")).as("consumption")
    ).select(expr("uuid()").as("id")
      , col("*")
      , lit("").as("label")
    )
      .drop("state_changed_group")

  }


}

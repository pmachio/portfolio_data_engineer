package org.uam.masterbigdata

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}

import java.sql.Timestamp
import scala.annotation.tailrec
import scala.collection.immutable.List

object EventsHelper {
  /** Creates a event based on the abusive throttle use (more than 20%) */
  def createExcessiveThrottleEvent()(df: DataFrame): DataFrame = {
    df.transform(CommonTelemetryHelper.flatBasicFields())
      .where(col("can").getField("vehicle").getField("pedals").getField("throttle").getField("level") >= 20)
      .withColumn("value", expr("concat( cast( can.vehicle.pedals.throttle.level as string ), '%' )"))
      .select(
        expr("uuid()").as("id")
        , col("device_id")
        , col("timestamp").as("created")
        , lit(1L).as("type_id")
        , col("location_address")
        , col("location_latitude")
        , col("location_longitude")
        , col("value")
      )
  }

  case class FuelStealingEventData(device_id: Long, timestamp: Timestamp, location_address: String, location_latitude: Double, location_longitude: Double, fuel_level: Int)

  case class FuelStealingEventState(device_id: Long, timestamp: Timestamp, location_address: String, location_latitude: Double, location_longitude: Double, fuel_level: Int)

  case class FuelStealingEventResponse(device_id: Long, timestamp: Timestamp, location_address: String, location_latitude: Double, location_longitude: Double, fuel_level_diff: Int)

  def createFuelStealingEvent()(df: DataFrame): DataFrame = {

    df.transform(CommonTelemetryHelper.flatBasicFields())
      //??tumbling window para solo procesar las que están en los 2 minutos actuales (¿separar? en otra función)
      //transformar que solo tener los campos necesarios para la case class
      .select(col("timestamp")
        , col("device_id")
        , col("location_address")
        , col("location_latitude")
        , col("location_longitude")
        , col("can").getField("fuel").getField("level").as("fuel_level")
      )
      //pasar a Dataset
      .as[FuelStealingEventData](Encoders.product[FuelStealingEventData])
      //aplicar groupByKey(deviceId)
      .groupByKey(_.device_id)(Encoders.scalaLong)
      //aplicar el procesamiento por estado
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateFuelStealingEvent)(Encoders.product[List[FuelStealingEventState]], Encoders.product[FuelStealingEventResponse])
      //Crear el evento
      .toDF()
      .select(
        expr("uuid()").as("id")
        , col("device_id")
        , col("timestamp").as("created")
        , lit(2L).as("type_id")
        , col("location_address")
        , col("location_latitude")
        , col("location_longitude")
        , lit("5% in less or 5 minutes").as("value") //sustituir
      )
  }

  private def updateFuelStealingEvent(
                                       deviceId: Long, // the key by which the grouping was made
                                       group: Iterator[FuelStealingEventData], // a batch of data associated to the key
                                       state: GroupState[List[FuelStealingEventState]] // like an "option", I have to manage manually
                                     ): Iterator[FuelStealingEventResponse] = {
    group.flatMap {
      record =>
      val states =
        if (state.exists) state.get
        else List()

      val result = checkFuelStealing(record, states)

      state.update(result._1)

      if(null != result._2) {
        Iterator(result._2)
      }else{
        Iterator()
      }
    }
  }

  /**Si los eventos no están ordenados correctamente en el tiempo no funcionará bien*/
  def checkFuelStealing(data: FuelStealingEventData, states: List[FuelStealingEventState]): (List[FuelStealingEventState], FuelStealingEventResponse) = {
    if (states.length == 0) {
      (states :+ FuelStealingEventState(data.device_id, data.timestamp, data.location_address, data.location_latitude, data.location_longitude, data.fuel_level)
        , null)
    } else {
      @tailrec
      def go(currentStates: List[FuelStealingEventState], result: (List[FuelStealingEventState], FuelStealingEventResponse)): (List[FuelStealingEventState], FuelStealingEventResponse) = {
        if (currentStates.isEmpty)
          ( fuelStealingEventDataToState(data) :: result._1
            , result._2)
        else {
          if ((data.timestamp.getTime - currentStates.head.timestamp.getTime) >= 5 * 60000) {
            if (currentStates.head.fuel_level - data.fuel_level >= 5) (fuelStealingEventDataToState(data) :: result._1
              , FuelStealingEventResponse(data.device_id, data.timestamp, data.location_address, data.location_latitude, data.location_longitude, currentStates.head.fuel_level - data.fuel_level))
            else (fuelStealingEventDataToState(data) :: result._1
              , null)
          }
          else go(
            currentStates.tail
            ,(result._1 :+ currentStates.head
              , null)
          )
        }
      }


      go(
        states, (List(), null)
      )
    }
  }

  private def fuelStealingEventDataToState(data: FuelStealingEventData): FuelStealingEventState =
    FuelStealingEventState(data.device_id, data.timestamp, data.location_address, data.location_latitude, data.location_longitude, data.fuel_level)
}

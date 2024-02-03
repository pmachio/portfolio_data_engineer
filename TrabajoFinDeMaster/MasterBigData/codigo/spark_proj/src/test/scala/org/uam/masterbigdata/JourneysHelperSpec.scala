package org.uam.masterbigdata

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.scalatest.funspec.AnyFunSpec

import java.util.UUID

class JourneysHelperSpec extends AnyFunSpec
  with DataFrameComparer
  with SparkSessionTestWrapper
  with DataFrameTestHelper
  with Schemas {

  describe("setIgnitionStateChange") {
    val source_schema: StructType = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("timestamp", TimestampType, nullable = false)
      )
    )

    val expected_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("state_changed", IntegerType)
      )
    )


    it("If the state of ignition doesn't change (always false) from previous frame to current the value is 0") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition doesn't change (always true) from previous frame to current the value is 0") {
      val sourceDF = jsonToDF(
        List("""{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition changes (from true to false) from previous frame to current the value is 1") {
      val sourceDF = jsonToDF(
        List("""{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("If the state of ignition changes (from false to true) from previous frame to current the value is 1") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }


    it("With 2 devices. If the state of ignition changes (from false to true) from previous frame to current the value is 1") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false,"device_id":2, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":false,"device_id":2, "timestamp":"2022-02-01 00:00:02"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true,"device_id":2, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04"}"""
          , """{"ignition":true,"device_id":2, "timestamp":"2022-02-01 00:00:04"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
          , """{"ignition":false,"device_id":2, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":2, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true,"device_id":2, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true,"device_id":2, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("With the unordered frames. If the state of ignition changes (from false to true) from previous frame to current the value is 1") {
      val sourceDF = jsonToDF(
        List("""{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01"}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04"}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02"}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setIgnitionStateChange()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , expected_schema
      )
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  describe("setGroupOfStateChangesToFrames") {
    val source_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("state_changed", IntegerType)
      )
    )

    val expected_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("state_changed", IntegerType)
        , StructField("state_changed_group", LongType)
      )
    )

    it("There is no state change so there is no increment in the group identifier") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setGroupOfStateChangesToFrames()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0, "state_changed_group":0}"""
        )
        , expected_schema
      )
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
    //con grupo
    it("There are two state changes so the group is increased by two") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:05", "state_changed":1}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:06", "state_changed":0}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setGroupOfStateChangesToFrames()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":1, "state_changed_group":1}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0, "state_changed_group":1}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:05", "state_changed":1, "state_changed_group":2}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:06", "state_changed":0, "state_changed_group":2}"""
        )
        , expected_schema
      )
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    //con grupo 2 dispositivos
    it("Two devices. There is one state change by device so there is one increment in the group identifier per device ") {
      val sourceDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
          , """{"ignition":false,"device_id":2, "timestamp":"2022-02-01 00:00:01", "state_changed":0}"""
          , """{"ignition":false,"device_id":2, "timestamp":"2022-02-01 00:00:02", "state_changed":0}"""
          , """{"ignition":true,"device_id":2, "timestamp":"2022-02-01 00:00:03", "state_changed":1}"""
          , """{"ignition":true,"device_id":2, "timestamp":"2022-02-01 00:00:04", "state_changed":0}"""
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setGroupOfStateChangesToFrames()(sourceDF)

      val expectedDF = jsonToDF(
        List("""{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:01", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false,"device_id":1, "timestamp":"2022-02-01 00:00:02", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:03", "state_changed":1, "state_changed_group":1}"""
          , """{"ignition":true,"device_id":1, "timestamp":"2022-02-01 00:00:04", "state_changed":0, "state_changed_group":1}"""
          , """{"ignition":false,"device_id":2, "timestamp":"2022-02-01 00:00:01", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":false,"device_id":2, "timestamp":"2022-02-01 00:00:02", "state_changed":0, "state_changed_group":0}"""
          , """{"ignition":true,"device_id":2, "timestamp":"2022-02-01 00:00:03", "state_changed":1, "state_changed_group":1}"""
          , """{"ignition":true,"device_id":2, "timestamp":"2022-02-01 00:00:04", "state_changed":0, "state_changed_group":1}"""
        )
        , expected_schema
      )
      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  describe("flatMainFields") {
    val source_schema: StructType = StructType(
      Array(
        StructField("timestamp", StringType, nullable = false)
        , StructField("attributes", StructType(
          Array(
            StructField("tenantId", StringType, nullable = false)
            , StructField("deviceId", StringType, nullable = false)
            , StructField("manufacturer", StringType, nullable = false)
            , StructField("model", StringType, nullable = false)
            , StructField("identifier", StringType, nullable = false)
          )
        ), nullable = false)
        , StructField("gnss", StructType(
          Array(
            StructField("type", StringType, nullable = false)
            , StructField("coordinate", StructType(
              Array(
                StructField("lat", DoubleType, nullable = false)
                , StructField("lng", DoubleType, nullable = false)
              )
            ))
            , StructField("altitude", DoubleType)
            , StructField("speed", IntegerType)
            , StructField("course", IntegerType)
            , StructField("address", StringType)
            , StructField("satellites", IntegerType)
          )
        ))
        , StructField("ignition", StructType(
          Array(
            StructField("status", BooleanType)
          )
        ))
      )
    )

    val expected_schema: StructType = StructType(
      Array(
        StructField("timestamp", TimestampType, nullable = false)
        , StructField("attributes", StructType(
          Array(
            StructField("tenantId", StringType, nullable = false)
            , StructField("manufacturer", StringType, nullable = false)
            , StructField("model", StringType, nullable = false)
            , StructField("identifier", StringType, nullable = false)
          )
        ), nullable = false)
        ,StructField("gnss", StructType(
          Array(
            StructField("type", StringType, nullable = false)
            , StructField("altitude", DoubleType)
            , StructField("speed", IntegerType)
            , StructField("course", IntegerType)
            , StructField("satellites", IntegerType)
          )
        ))
        , StructField("ignition", BooleanType, nullable = false)
        ,StructField("device_id", LongType, nullable = false)
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
      )
    )

    it("Flats the main fields for the journeys analisis") {
      val sourceDF = jsonToDF(
        List(
          """{"timestamp":"2023-02-05T10:58:27Z"
            |,"attributes": {
            |           "tenantId": "763738558589566976"
            |           , "deviceId": "1328414834680696832"
            |           , "manufacturer": "Teltonika"
            |           , "model": "TeltonikaFMB001"
            |           , "identifier": "352094083025970TSC"
            |}
            |,"gnss":{
            |          "type":"Gps"
            |          ,"coordinate":{
            |                          "lat":40.605956
            |                          ,"lng":-3.711923
            |                        }
            |          ,"altitude":722.0
            |          ,"speed":0
            |          ,"course":212
            |          ,"address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a"
            |          ,"satellites":13
            |          }
            |,"ignition":{"status":false}}""".stripMargin
        )
        , source_schema)

      val actualDF = JourneysHelper.flatMainFields()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"timestamp":"2023-02-05 10:58:27"
            |,"attributes": {
            |           "tenantId": "763738558589566976"
            |           , "manufacturer": "Teltonika"
            |           , "model": "TeltonikaFMB001"
            |           , "identifier": "352094083025970TSC"
            |}
            |,"gnss":{
            |          "type":"Gps"
            |          ,"altitude":722.0
            |          ,"speed":0
            |          ,"course":212
            |          ,"satellites":13
            |          }
            |,"ignition":false
            |,"device_id":1328414834680696832
            |,"location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a"
            |,"location_latitude":40.605956
            |,"location_longitude":-3.711923}""".stripMargin
        )
        , expected_schema)

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  describe("setInitialStateChangeValues") {
    val source_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
      )
    )
    val expected_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
        , StructField("start_timestamp", TimestampType, nullable = false)
        , StructField("start_location_address", StringType, nullable = false)
        , StructField("start_location_latitude", DoubleType, nullable = false)
        , StructField("start_location_longitude", DoubleType, nullable = false)
      )
    )

    it(
      """It sets the timestamp, location_address, location_latitude, location_longitude of the first frame (older timestamp) of frames with
         the same state_changed_group
        """) {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2}""".stripMargin
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setInitialStateChangeValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1
            | , "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4}""".stripMargin
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it(
      """It sets the timestamp, location_address, location_latitude, location_longitude of the first frame (older timestamp) of frames with
         the same state_changed_group for two devices
        """) {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2}""".stripMargin
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setInitialStateChangeValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1
            | , "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1
            | , "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4}""".stripMargin
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  describe("setFinalStateChangeValues") {
    val source_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
      )
    )
    val expected_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("location_address", StringType, nullable = false)
        , StructField("location_latitude", DoubleType, nullable = false)
        , StructField("location_longitude", DoubleType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
        , StructField("end_timestamp", TimestampType, nullable = false)
        , StructField("end_location_address", StringType, nullable = false)
        , StructField("end_location_latitude", DoubleType, nullable = false)
        , StructField("end_location_longitude", DoubleType, nullable = false)
      )
    )
    it(
      """It sets the timestamp, location_address, location_latitude, location_longitude of the last frame (newer timestamp) of frames with
             the same state_changed_group
            """) {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2}""".stripMargin
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setFinalStateChangeValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1
            | , "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4}""".stripMargin
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it(
      """It sets the timestamp, location_address, location_latitude, location_longitude of the last frame (newer timestamp) of frames with
             the same state_changed_group. Apply to two devices
            """) {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2}""".stripMargin
        )
        , source_schema
      )

      val actualDF = JourneysHelper.setFinalStateChangeValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1
            | , "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:01", "location_address":"address1", "location_latitude":1.1, "location_longitude":2.1
            |, "state_changed":0, "state_changed_group":0
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:02", "location_address":"address2", "location_latitude":1.2, "location_longitude":2.2
            |, "state_changed":0, "state_changed_group":1
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            | ,"timestamp":"2022-02-01 00:00:03", "location_address":"address3", "location_latitude":1.3, "location_longitude":2.3
            | , "state_changed":0, "state_changed_group":1
            | , "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "timestamp":"2022-02-01 00:00:04", "location_address":"address4", "location_latitude":1.4, "location_longitude":2.4
            |, "state_changed":0, "state_changed_group":2
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4}""".stripMargin
        )
        , expected_schema
      )

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  describe("setCountersValues") {
    val source_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("can", StructType(
          Array(
            StructField("vehicle", StructType(
              Array(
                StructField("mileage", StructType(
                  Array(
                    StructField("distance", LongType)
                  )
                ))
              )
            ))
            , StructField("fuel", StructType(
              Array(
                StructField("consumed", StructType(
                  Array(
                    StructField("volume", LongType)
                  )
                ))
              )
            ))
          )
        ))
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
        , StructField("start_timestamp", TimestampType, nullable = false)
        , StructField("start_location_address", StringType, nullable = false)
        , StructField("start_location_latitude", DoubleType, nullable = false)
        , StructField("start_location_longitude", DoubleType, nullable = false)
        , StructField("end_timestamp", TimestampType, nullable = false)
        , StructField("end_location_address", StringType, nullable = false)
        , StructField("end_location_latitude", DoubleType, nullable = false)
        , StructField("end_location_longitude", DoubleType, nullable = false)
      )
    )

    val expected_schema = StructType(
      Array(
        StructField("ignition", BooleanType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("can", StructType(
          Array(
            StructField("vehicle", StructType(
              Array(
                StructField("mileage", StructType(
                  Array(
                    StructField("distance", LongType)
                  )
                ))
              )
            ))
            , StructField("fuel", StructType(
              Array(
                StructField("consumed", StructType(
                  Array(
                    StructField("volume", LongType)
                  )
                ))
              )
            ))
          )
        ))
        , StructField("timestamp", TimestampType, nullable = false)
        , StructField("state_changed", IntegerType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
        , StructField("start_timestamp", TimestampType, nullable = false)
        , StructField("start_location_address", StringType, nullable = false)
        , StructField("start_location_latitude", DoubleType, nullable = false)
        , StructField("start_location_longitude", DoubleType, nullable = false)
        , StructField("end_timestamp", TimestampType, nullable = false)
        , StructField("end_location_address", StringType, nullable = false)
        , StructField("end_location_latitude", DoubleType, nullable = false)
        , StructField("end_location_longitude", DoubleType, nullable = false)
        , StructField("distance", LongType, nullable = false)
        , StructField("consumption", LongType, nullable = false)
      )
    )
    it("Aggregates values of the different StateChange of a device") {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1000 } }, "fuel":{"consumed": {"volume": 100 } } }
            |, "timestamp":"2022-02-01 00:00:01"
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1200 } }, "fuel":{"consumed": {"volume": 120 } } }
            |, "timestamp":"2022-02-01 00:00:02"
            |, "state_changed":0, "state_changed_group":1
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1300 } }, "fuel":{"consumed": {"volume": 130 } } }
            |, "timestamp":"2022-02-01 00:00:03"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1400 } }, "fuel":{"consumed": {"volume": 140 } } }
            |, "timestamp":"2022-02-01 00:00:04"
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4}""".stripMargin
        )
        , source_schema)

      val actualDF = JourneysHelper.setCountersValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1000 } }, "fuel":{"consumed": {"volume": 100 } } }
            |, "timestamp":"2022-02-01 00:00:01"
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1200 } }, "fuel":{"consumed": {"volume": 120 } } }
            |, "timestamp":"2022-02-01 00:00:02"
            |, "state_changed":0, "state_changed_group":1
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1300 } }, "fuel":{"consumed": {"volume": 130 } } }
            |, "timestamp":"2022-02-01 00:00:03"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":100, "consumption":10 }""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1400 } }, "fuel":{"consumed": {"volume": 140 } } }
            |, "timestamp":"2022-02-01 00:00:04"
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":0, "consumption":0 }""".stripMargin
        )
        , expected_schema)

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("Aggregates values of the different StateChange of two devices") {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1000 } }, "fuel":{"consumed": {"volume": 100 } } }
            |, "timestamp":"2022-02-01 00:00:01"
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "can": { "vehicle":{"mileage": {"distance": 2000 } }, "fuel":{"consumed": {"volume": 200 } } }
            |, "timestamp":"2022-02-01 00:00:01"
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1200 } }, "fuel":{"consumed": {"volume": 120 } } }
            |, "timestamp":"2022-02-01 00:00:02"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "can": { "vehicle":{"mileage": {"distance": 2200 } }, "fuel":{"consumed": {"volume": 220 } } }
            |, "timestamp":"2022-02-01 00:00:02"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1300 } }, "fuel":{"consumed": {"volume": 130 } } }
            |, "timestamp":"2022-02-01 00:00:03"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "can": { "vehicle":{"mileage": {"distance": 2400 } }, "fuel":{"consumed": {"volume": 240 } } }
            |, "timestamp":"2022-02-01 00:00:03"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1400 } }, "fuel":{"consumed": {"volume": 140 } } }
            |, "timestamp":"2022-02-01 00:00:04"
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4}""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "can": { "vehicle":{"mileage": {"distance": 2500 } }, "fuel":{"consumed": {"volume": 250 } } }
            |, "timestamp":"2022-02-01 00:00:04"
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4}""".stripMargin
        )
        , source_schema)

      val actualDF = JourneysHelper.setCountersValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1000 } }, "fuel":{"consumed": {"volume": 100 } } }
            |, "timestamp":"2022-02-01 00:00:01"
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1200 } }, "fuel":{"consumed": {"volume": 120 } } }
            |, "timestamp":"2022-02-01 00:00:02"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1300 } }, "fuel":{"consumed": {"volume": 130 } } }
            |, "timestamp":"2022-02-01 00:00:03"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":100, "consumption":10 } """.stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "can": { "vehicle":{"mileage": {"distance": 1400 } }, "fuel":{"consumed": {"volume": 140 } } }
            |, "timestamp":"2022-02-01 00:00:04"
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":0, "consumption":0 } """.stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "can": { "vehicle":{"mileage": {"distance": 2000 } }, "fuel":{"consumed": {"volume": 200 } } }
            |, "timestamp":"2022-02-01 00:00:01"
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "can": { "vehicle":{"mileage": {"distance": 2200 } }, "fuel":{"consumed": {"volume": 220 } } }
            |, "timestamp":"2022-02-01 00:00:02"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":0, "consumption":0 } """.stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "can": { "vehicle":{"mileage": {"distance": 2400 } }, "fuel":{"consumed": {"volume": 240 } } }
            |, "timestamp":"2022-02-01 00:00:03"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":200, "consumption":20 } """.stripMargin
          ,
          """{"ignition":false,"device_id":2
            |, "can": { "vehicle":{"mileage": {"distance": 2500 } }, "fuel":{"consumed": {"volume": 250 } } }
            |, "timestamp":"2022-02-01 00:00:04"
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":0, "consumption":0 } """.stripMargin
        )
        , expected_schema)

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("Handle values when canbus fields are missing") {
      val sourceDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01"
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:03"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3}""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04"
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4}""".stripMargin
        )
        , source_schema)

      val actualDF = JourneysHelper.setCountersValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:01"
            |, "state_changed":0, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:02"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:03"
            |, "state_changed":0, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"ignition":false,"device_id":1
            |, "timestamp":"2022-02-01 00:00:04"
            |, "state_changed":0, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":0, "consumption":0 }""".stripMargin
        )
        , expected_schema)


      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }


  describe("aggregateStateChangeValues") {
    val source_schema = StructType(
      Array(
         StructField("device_id", LongType, nullable = false)
        , StructField("state_changed_group", LongType, nullable = false)
        , StructField("start_timestamp", TimestampType, nullable = false)
        , StructField("start_location_address", StringType, nullable = false)
        , StructField("start_location_latitude", DoubleType, nullable = false)
        , StructField("start_location_longitude", DoubleType, nullable = false)
        , StructField("end_timestamp", TimestampType, nullable = false)
        , StructField("end_location_address", StringType, nullable = false)
        , StructField("end_location_latitude", DoubleType, nullable = false)
        , StructField("end_location_longitude", DoubleType, nullable = false)
        , StructField("distance", LongType, nullable = false)
        , StructField("consumption", LongType, nullable = false)
      )
    )
    val expected_schema = StructType(
      Array(
        StructField("id", StringType, nullable = false)
        , StructField("device_id", LongType, nullable = false)
        , StructField("start_timestamp", TimestampType, nullable = false)
        , StructField("start_location_address", StringType, nullable = false)
        , StructField("start_location_latitude", DoubleType, nullable = false)
        , StructField("start_location_longitude", DoubleType, nullable = false)
        , StructField("end_timestamp", TimestampType, nullable = false)
        , StructField("end_location_address", StringType, nullable = false)
        , StructField("end_location_latitude", DoubleType, nullable = false)
        , StructField("end_location_longitude", DoubleType, nullable = false)
        , StructField("distance", LongType, nullable = false)
        , StructField("consumption", LongType, nullable = false)
        , StructField("label", StringType, nullable = false)
      )
    )

    it("Aggregates values of the different StateChange of a device") {
      val sourceDF = jsonToDF(
        List(
          """{"device_id":1
            |, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"device_id":1
            |, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"device_id":1
            |, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":100, "consumption":10 }""".stripMargin
          ,
          """{"device_id":1
            |, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":0, "consumption":0 }""".stripMargin
        )
        , source_schema)

      val actualDF = JourneysHelper.aggregateStateChangeValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"id":"a", "device_id":1
            |, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":0, "consumption":0, "label":"" }""".stripMargin
          ,
          """{"id":"b","device_id":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":100, "consumption":10, "label":"" }""".stripMargin
          ,
          """{"id":"c","device_id":1
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":0, "consumption":0, "label":"" }""".stripMargin
        )
        , expected_schema)


      //quitamos el id, ya que no podemos crear el mismo UUID en ambos dataframes
      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }

    it("Aggregates values of the different StateChange of two devices") {
      val sourceDF = jsonToDF(
        List(
          """{"device_id":1
            |, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"device_id":2
            |, "state_changed_group":0
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":90, "consumption":9 }""".stripMargin
          ,
          """{"device_id":1
            |, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"device_id":2
            |, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":50, "consumption":5 }""".stripMargin
          ,
          """{"device_id":1
            |, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":100, "consumption":10 }""".stripMargin
          ,
          """{"device_id":2
            |, "state_changed_group":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":150, "consumption":15 }""".stripMargin
          ,
          """{"device_id":1
            |, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":0, "consumption":0 }""".stripMargin
          ,
          """{"device_id":2
            |, "state_changed_group":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":100, "consumption":10 }""".stripMargin
        )
        , source_schema)

      val actualDF = JourneysHelper.aggregateStateChangeValues()(sourceDF)

      val expectedDF = jsonToDF(
        List(
          """{"id":"a", "device_id":1
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":0, "consumption":0, "label":"" }""".stripMargin
          ,
          """{"id":"b","device_id":2
            |, "start_timestamp":"2022-02-01 00:00:01", "start_location_address":"address1", "start_location_latitude":1.1, "start_location_longitude":2.1
            |, "end_timestamp":"2022-02-01 00:00:01", "end_location_address":"address1", "end_location_latitude":1.1, "end_location_longitude":2.1
            |, "distance":90, "consumption":9, "label":"" }""".stripMargin
          ,
          """{"id":"c","device_id":1
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":100, "consumption":10, "label":"" }""".stripMargin
          ,
          """{"id":"d", "device_id":2
            |, "start_timestamp":"2022-02-01 00:00:02", "start_location_address":"address2", "start_location_latitude":1.2, "start_location_longitude":2.2
            |, "end_timestamp":"2022-02-01 00:00:03", "end_location_address":"address3", "end_location_latitude":1.3, "end_location_longitude":2.3
            |, "distance":200, "consumption":20, "label":"" }""".stripMargin
          ,
          """{"id":"e", "device_id":1
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":0, "consumption":0, "label":"" }""".stripMargin
          ,
          """{"id":"f", "device_id":2
            |, "start_timestamp":"2022-02-01 00:00:04", "start_location_address":"address4", "start_location_latitude":1.4, "start_location_longitude":2.4
            |, "end_timestamp":"2022-02-01 00:00:04", "end_location_address":"address4", "end_location_latitude":1.4, "end_location_longitude":2.4
            |, "distance":100, "consumption":10, "label":"" }""".stripMargin
        )
        , expected_schema)

      //quitamos el id, ya que no podemos crear el mismo UUID en ambos dataframes
      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }
  }


  describe("calculateJourneys") {
    it("If ignition changes from on and off a journey is created") {
      val expectedDF = jsonToDF(List(
        s"""{"id":"${UUID.randomUUID().toString}", "device_id":1328414834680696832
           |,"start_timestamp":"2023-02-05 10:55:27" ,"start_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1"
           |,"start_location_latitude":40.605957 ,"start_location_longitude":-3.711923
           |,"end_timestamp":"2023-02-05 10:57:27" ,"end_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3"
           |,"end_location_latitude":40.605959, "end_location_longitude":-3.711921
           |,"distance":0, "consumption":0 ,"label":""}""".stripMargin
      ), journey_schema)

      val sourceDF = jsonToDF(
        List(
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:54:27Z"
            , "server": {"timestamp": "2023-02-05T10:54:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps","coordinate":{ "lat":40.605956 ,"lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": false}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:55:27Z"
            , "server": {"timestamp": "2023-02-05T10:55:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": { "type": "Gps", "coordinate": { "lat":40.605957, "lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:56:27Z"
            , "server": {"timestamp": "2023-02-05T10:56:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps", "coordinate": {"lat":40.605958, "lng" :-3.711922}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a2", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:57:27Z"
            , "server": {"timestamp": "2023-02-05T10:57:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps", "coordinate": {"lat":40.605959, "lng" :-3.711921}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:58:27Z"
            , "server": { "timestamp": "2023-02-05T10:58:28.808Z"}
            , "attributes": { "tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC" }
            , "gnss": { "type": "Gps", "coordinate": { "lat":40.605956, "lng" :-3.711923}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13 }
            , "ignition": { "status": false } }"""
        )
        , telemetry_schema)

      val actualDF = JourneysHelper.calculateJourneys()(sourceDF)

      //para comparar quitamos los id porque se generan de forma independiente y no van a coincidir
      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }

    it("If ignition just on, a journey is created") {
      val expectedDF = jsonToDF(List(
        s"""{"id":"${UUID.randomUUID().toString}", "device_id":1328414834680696832
           |,"start_timestamp":"2023-02-05 10:54:27" ,"start_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a"
           |,"start_location_latitude":40.605956 ,"start_location_longitude":-3.711923
           |,"end_timestamp":"2023-02-05 10:58:27" ,"end_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a4"
           |,"end_location_latitude":40.605956, "end_location_longitude":-3.711923
           |,"distance":0, "consumption":0 ,"label":""}""".stripMargin
      ), journey_schema)

      val sourceDF = jsonToDF(
        List(
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:54:27Z"
              , "server": {"timestamp": "2023-02-05T10:54:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps","coordinate":{ "lat":40.605956 ,"lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": true}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:55:27Z"
              , "server": {"timestamp": "2023-02-05T10:55:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": { "type": "Gps", "coordinate": { "lat":40.605957, "lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": true}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:56:27Z"
              , "server": {"timestamp": "2023-02-05T10:56:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "coordinate": {"lat":40.605958, "lng" :-3.711922}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a2", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": true}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:57:27Z"
              , "server": {"timestamp": "2023-02-05T10:57:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "coordinate": {"lat":40.605959, "lng" :-3.711921}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": true}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:58:27Z"
              , "server": { "timestamp": "2023-02-05T10:58:28.808Z"}
              , "attributes": { "tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC" }
              , "gnss": { "type": "Gps", "coordinate": { "lat":40.605956, "lng" :-3.711923}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a4", "precision": "Ideal", "satellites": 13 }
              , "ignition": { "status": true } }"""
        )
        , telemetry_schema)

      val actualDF = JourneysHelper.calculateJourneys()(sourceDF)

      //para comparar quitamos los id porque se generan de forma independiente y no van a coincidir
      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }

    it("If ignition just off, no journey is created") {
      val sourceDF = jsonToDF(
        List(
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:54:27Z"
              , "server": {"timestamp": "2023-02-05T10:54:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps","coordinate":{ "lat":40.605956 ,"lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": false}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:55:27Z"
              , "server": {"timestamp": "2023-02-05T10:55:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": { "type": "Gps", "coordinate": { "lat":40.605957, "lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": false}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:56:27Z"
              , "server": {"timestamp": "2023-02-05T10:56:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "coordinate": {"lat":40.605958, "lng" :-3.711922}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a2", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": false}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:57:27Z"
              , "server": {"timestamp": "2023-02-05T10:57:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "coordinate": {"lat":40.605959, "lng" :-3.711921}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": false}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:58:27Z"
              , "server": { "timestamp": "2023-02-05T10:58:28.808Z"}
              , "attributes": { "tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC" }
              , "gnss": { "type": "Gps", "coordinate": { "lat":40.605956, "lng" :-3.711923}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13 }
              , "ignition": { "status": false } }"""
        )
        , telemetry_schema)

      val actualDF = JourneysHelper.calculateJourneys()(sourceDF)

      assert(actualDF.count() === 0L)
    }

    it("If there is no coordinates no journey is created") {
      val expectedDF = jsonToDF(List(
        s"""{"id":"${UUID.randomUUID().toString}", "device_id":1328414834680696832
           |,"start_timestamp":"2023-02-05 10:54:27" ,"start_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a"
           |,"start_location_latitude":40.605956 ,"start_location_longitude":-3.711923
           |,"end_timestamp":"2023-02-05 10:58:27" ,"end_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a4"
           |,"end_location_latitude":40.605956, "end_location_longitude":-3.711923
           |,"distance":0, "consumption":0 ,"label":""}""".stripMargin
      ), journey_schema)

      val sourceDF = jsonToDF(
        List(
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:54:27Z"
              , "server": {"timestamp": "2023-02-05T10:54:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": true}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:55:27Z"
              , "server": {"timestamp": "2023-02-05T10:55:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": { "type": "Gps",  "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": true}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:56:27Z"
              , "server": {"timestamp": "2023-02-05T10:56:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a2", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": true}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:57:27Z"
              , "server": {"timestamp": "2023-02-05T10:57:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": true}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:58:27Z"
              , "server": { "timestamp": "2023-02-05T10:58:28.808Z"}
              , "attributes": { "tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC" }
              , "gnss": { "type": "Gps", "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a4", "precision": "Ideal", "satellites": 13 }
              , "ignition": { "status": true } }"""
        )
        , telemetry_schema)

      val actualDF = JourneysHelper.calculateJourneys()(sourceDF)

      assert(0 === actualDF.count())
    }
  }

  describe("calculateLabeledJourneys"){
    it("Calculates the journeys and set the their label if is unknown"){
      val expectedDF = jsonToDF(List(
        s"""{"id":"${UUID.randomUUID().toString}", "device_id":1328414834680696832
           |,"start_timestamp":"2023-02-05 10:55:27" ,"start_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1"
           |,"start_location_latitude":40.605957 ,"start_location_longitude":-3.711923
           |,"end_timestamp":"2023-02-05 10:57:27" ,"end_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3"
           |,"end_location_latitude":40.605959, "end_location_longitude":-3.711921
           |,"distance":0, "consumption":0 ,"label":"unknown"}""".stripMargin
      ), journey_schema)

      val sourceDF = jsonToDF(
        List(
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:54:27Z"
            , "server": {"timestamp": "2023-02-05T10:54:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps","coordinate":{ "lat":40.605956 ,"lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": false}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:55:27Z"
            , "server": {"timestamp": "2023-02-05T10:55:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": { "type": "Gps", "coordinate": { "lat":40.605957, "lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:56:27Z"
            , "server": {"timestamp": "2023-02-05T10:56:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps", "coordinate": {"lat":40.605958, "lng" :-3.711922}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a2", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:57:27Z"
            , "server": {"timestamp": "2023-02-05T10:57:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps", "coordinate": {"lat":40.605959, "lng" :-3.711921}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:58:27Z"
            , "server": { "timestamp": "2023-02-05T10:58:28.808Z"}
            , "attributes": { "tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC" }
            , "gnss": { "type": "Gps", "coordinate": { "lat":40.605956, "lng" :-3.711923}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13 }
            , "ignition": { "status": false } }"""
        )
        , telemetry_schema)

      val actualDF = JourneysHelper.calculateLabeledJourneys("../entorno/data/journeys_logreg_cv")(sourceDF)

      //para comparar quitamos los id porque se generan de forma independiente y no van a coincidir
      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }

    it("Calculates the journeys and set the their label if is known") {
      val expectedDF = jsonToDF(List(
        s"""{"id":"${UUID.randomUUID().toString}", "device_id":1328414834680696832
           |,"start_timestamp":"2023-02-05 10:55:27" ,"start_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1"
           |,"start_location_latitude":40.614105 ,"start_location_longitude":-3.711923
           |,"end_timestamp":"2023-02-05 10:57:27" ,"end_location_address":"Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3"
           |,"end_location_latitude":40.6804, "end_location_longitude":-3.976797
           |,"distance":0, "consumption":0 ,"label":"Tres Cantos - Moralzarzal"}""".stripMargin
      ), journey_schema)


      val sourceDF = jsonToDF(
        List(
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:54:27Z"
            , "server": {"timestamp": "2023-02-05T10:54:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps","coordinate":{ "lat":40.605956 ,"lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": false}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:55:27Z"
            , "server": {"timestamp": "2023-02-05T10:55:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": { "type": "Gps", "coordinate": { "lat":40.614105, "lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:56:27Z"
            , "server": {"timestamp": "2023-02-05T10:56:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps", "coordinate": {"lat":40.605958, "lng" :-3.711922}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a2", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:57:27Z"
            , "server": {"timestamp": "2023-02-05T10:57:28.808Z"}
            , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
            , "gnss": {"type": "Gps", "coordinate": {"lat":40.6804, "lng" :-3.976797}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3", "precision": "Ideal", "satellites": 13}
            , "ignition": {"status": true}}"""
          ,
          """{
            "id": 1622186900238446592
            , "version": "1"
            , "timestamp": "2023-02-05T10:58:27Z"
            , "server": { "timestamp": "2023-02-05T10:58:28.808Z"}
            , "attributes": { "tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC" }
            , "gnss": { "type": "Gps", "coordinate": { "lat":40.605956, "lng" :-3.711923}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13 }
            , "ignition": { "status": false } }"""
        )
        , telemetry_schema)

      val actualDF = JourneysHelper.calculateLabeledJourneys("../entorno/data/journeys_logreg_cv")(sourceDF)

      //para comparar quitamos los id porque se generan de forma independiente y no van a coincidir
      assertSmallDataFrameEquality(actualDF.drop("id"), expectedDF.drop("id"), ignoreNullable = true)
    }

    it("If there are no journeys nothing is done") {
      val sourceDF = jsonToDF(
        List(
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:54:27Z"
              , "server": {"timestamp": "2023-02-05T10:54:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps","coordinate":{ "lat":40.605956 ,"lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": false}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:55:27Z"
              , "server": {"timestamp": "2023-02-05T10:55:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": { "type": "Gps", "coordinate": { "lat":40.605957, "lng":-3.711923 }, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a1", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": false}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:56:27Z"
              , "server": {"timestamp": "2023-02-05T10:56:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "coordinate": {"lat":40.605958, "lng" :-3.711922}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a2", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": false}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:57:27Z"
              , "server": {"timestamp": "2023-02-05T10:57:28.808Z"}
              , "attributes": {"tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC"}
              , "gnss": {"type": "Gps", "coordinate": {"lat":40.605959, "lng" :-3.711921}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a3", "precision": "Ideal", "satellites": 13}
              , "ignition": {"status": false}}"""
          ,
          """{
              "id": 1622186900238446592
              , "version": "1"
              , "timestamp": "2023-02-05T10:58:27Z"
              , "server": { "timestamp": "2023-02-05T10:58:28.808Z"}
              , "attributes": { "tenantId": "763738558589566976", "deviceId": "1328414834680696832", "manufacturer": "Teltonika", "model": "TeltonikaFMB001", "identifier": "352094083025970TSC" }
              , "gnss": { "type": "Gps", "coordinate": { "lat":40.605956, "lng" :-3.711923}, "altitude": 722.0, "speed": 0, "speedLimit": 50, "course": 212, "address": "Avenida de la Vega, Tres Cantos, Comunidad de Madrid, 28760, Espa�a", "precision": "Ideal", "satellites": 13 }
              , "ignition": { "status": false } }"""
        )
        , telemetry_schema)

      val actualDF = JourneysHelper.calculateLabeledJourneys("../entorno/data/journeys_logreg_cv")(sourceDF)

      assert(actualDF.count() === 0L)
    }
  }

}

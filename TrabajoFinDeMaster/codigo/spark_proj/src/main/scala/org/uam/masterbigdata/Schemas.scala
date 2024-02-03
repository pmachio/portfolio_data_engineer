package org.uam.masterbigdata

import org.apache.spark.sql.types.{BooleanType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

trait Schemas {

  val telemetry_schema: StructType = StructType(
    Array(
      StructField("id", LongType, nullable = false)
      , StructField("version", StringType, nullable = false)
      , StructField("timestamp", StringType, nullable = false)
      , StructField("server", StructType(
        Array(
          StructField("timestamp", StringType, nullable = false)
        )
      )
      )
      , StructField("attributes", StructType(
        Array(
          StructField("tenantId", StringType, nullable = false)
          , StructField("deviceId", StringType, nullable = false)
          , StructField("manufacturer", StringType, nullable = false)
          , StructField("model", StringType, nullable = false)
          , StructField("identifier", StringType, nullable = false)
        )
      ), nullable = false)
      , StructField("device", StructType(
        Array(
          StructField("battery", StructType(
            Array(
              StructField("voltage", LongType)
              , StructField("level", LongType)
            )
          ))
          , StructField("mileage", StructType(
            Array(
              StructField("distance", LongType)
            )
          ))
        )
      ), nullable = true)
      , StructField("can", StructType(
        Array(
          StructField("vehicle", StructType(
            Array(
              StructField("mileage", StructType(
                Array(
                  StructField("distance", LongType)
                )
              ))
              , StructField("pedals", StructType(Array(StructField("throttle", StructType(Array(StructField("level",IntegerType)))))))
              , StructField("cruise", StructType(
                Array(
                  StructField("status", BooleanType)
                )
              ))
              , StructField("handBrake", StructType(
                Array(
                  StructField("status", BooleanType)
                )
              ))
              , StructField("doors", StructType(
                Array(
                  StructField("indicator", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("lights", StructType(
                Array(
                  StructField("hazard", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                  , StructField("fog", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("seatBelts", StructType(
                Array(
                  StructField("indicator", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("beams", StructType(
                Array(
                  StructField("high", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("lock", StructType(
                Array(
                  StructField("central", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("gear", StructType(
                Array(
                  StructField("reverse", StructType(
                    Array(
                      StructField("status", BooleanType)
                    )
                  ))
                )
              ))
              , StructField("airConditioning", StructType(
                Array(
                  StructField("status", BooleanType)
                )
              ))
            )
          )
          )
          , StructField("engine", StructType(
            Array(
              StructField("time", StructType(
                Array(
                  StructField("duration", LongType)
                )
              ))
            )
          ))
          , StructField("battery", StructType(
            Array(
              StructField("charging", StructType(
                Array(
                  StructField("status", BooleanType)
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
              ,StructField("level", IntegerType)
            )
          ))
        )
      ), nullable = true)
      , StructField("gnss", StructType(
        Array(
          StructField("type", StringType, nullable = false)
          , StructField("coordinate", StructType(
            Array(
              StructField("lat", DoubleType, nullable = false)
              , StructField("lng", DoubleType, nullable = false)
            )
          )
          )
          , StructField("altitude", DoubleType)
          , StructField("speed", IntegerType)
          , StructField("course", IntegerType)
          , StructField("address", StringType)
          , StructField("satellites", IntegerType)
        )
      ), nullable = true)
      , StructField("ignition", StructType(
        Array(
          StructField("status", BooleanType)
        )
      ), nullable = true)
    )
  )


  val journey_schema:StructType = StructType(
    Array(
      StructField("id", StringType, nullable = false)
      ,StructField("device_id", LongType, nullable = false)
      ,StructField("start_timestamp", TimestampType, nullable = false)
      ,StructField("start_location_address", StringType, nullable = false)
      ,StructField("start_location_latitude", DoubleType, nullable = false)
      ,StructField("start_location_longitude", DoubleType, nullable = false)
      ,StructField("end_timestamp", TimestampType, nullable = false)
      ,StructField("end_location_address", StringType, nullable = false)
      ,StructField("end_location_latitude", DoubleType, nullable = false)
      ,StructField("end_location_longitude", DoubleType, nullable = false)
      ,StructField("distance", LongType, nullable = true)
      ,StructField("consumption", LongType, nullable = true)
      ,StructField("label", StringType, nullable = true)
    )
  )

  val event_schema:StructType = StructType(
    Array(
      StructField("id", StringType, nullable = false)
      , StructField("device_id", LongType, nullable = false)
      , StructField("created", TimestampType, nullable = false)
      , StructField("type_id", LongType, nullable = false)
      , StructField("location_address", StringType, nullable = false)
      , StructField("location_latitude", DoubleType, nullable = false)
      , StructField("location_longitude", DoubleType, nullable = false)
      , StructField("value", StringType, nullable = false)
    )
  )
}

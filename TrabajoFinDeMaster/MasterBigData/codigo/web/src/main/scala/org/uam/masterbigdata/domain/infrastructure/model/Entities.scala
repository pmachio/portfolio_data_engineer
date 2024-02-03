package org.uam.masterbigdata.domain.infrastructure.model

object Entities {
  case class JourneyDbo(
                      id: String
                      , device_id: Long
                      , start_timestamp: Long
                      , start_location_address: String
                      , start_location_latitude: Float
                      , start_location_longitude: Float
                      , end_timestamp: Long
                      , end_location_address: String
                      , end_location_latitude: Float
                      , end_location_longitude: Float
                      , distance: Long
                      , consumption:Long
                      , label:String
                    )

  case class EventDbo(
                         id: String
                         , device_id: Long
                         , created: Long
                         , type_id: Long
                         , location_address: Option[String]
                         , location_latitude: Option[Float]
                         , location_longitude: Option[Float]
                         , value: String
                       )

  case class FrameDbo(
                         id: Long
                         , device_id: Long
                         , created: Long
                         , received: Long
                         , location_created: Long
                         , location_address: String
                         , location_latitude: Float
                         , location_longitude: Float
                         , location_altitude: Float
                         , location_speed: Float
                         , location_valid: Boolean
                         , location_course: Float
                         , ignition: Boolean
                       )

}

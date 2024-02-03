package org.uam.masterbigdata.domain.model

object Entities {
  case class Journey(
                          id: String
                          , device_id: String
                          , start_timestamp: String
                          , start_location_address: String
                          , start_location_latitude: String
                          , start_location_longitude: String
                          , end_timestamp: String
                          , end_location_address: String
                          , end_location_latitude: String
                          , end_location_longitude: String
                          , distance: String
                          , consumption: String
                          , label:String
                        )
  case class JourneysByDeviceIdRequest(deviceId:Long)
  case class JourneysByDeviceIdAndLabelRequest(deviceId:Long, label:String)
  case class JourneyByDeviceIdRequest(deviceId:Long, journeyId:String)

  //Events
  case class Event(
                    id: String
                    , device_id: String
                    , created: String
                    , type_id: String
                    , location_address: String
                    , location_latitude: String
                    , location_longitude: String
                    , value: String
                  )

  case class EventsByDeviceIdRequest(deviceId: Long)

  case class EventByDeviceIdAndEventIdRequest(deviceId: Long, id: String)
  //Frames
  case class Frame(
                      id: String
                      , device_id: String
                      , created: String
                      , received: String
                      , location_created: String
                      , location_address: String
                      , location_latitude: String
                      , location_longitude: String
                      , location_altitude: String
                      , location_speed: String
                      , location_valid: String
                      , location_course: String
                      , ignition: String
                    )

  case class FramesByDeviceIdRequest(deviceId: Long)

  case class FramesByDeviceIdAndFrameIdRequest(deviceId: Long, id: Long)
}

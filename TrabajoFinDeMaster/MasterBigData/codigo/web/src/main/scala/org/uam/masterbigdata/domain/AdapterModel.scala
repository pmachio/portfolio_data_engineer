package org.uam.masterbigdata.domain

object AdapterModel {
  case class LocationView(
                           created: String
                           , address: String
                           , latitude: String
                           , longitude: String
                           , altitude: String
                           , speed: String
                           , valid: String
                           , course: String
                         )

  case class JourneyView(
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
                          , label: String
                        )

  case class FrameView(
                        id: String
                        , device_id: String
                        , created: String
                        , received: String
                        , location: LocationView
                        , ignition: String
                      )

  case class EventView(
                        id: String
                        , device_id: String
                        , created: String
                        , type_id: String
                        , location_address: String
                        , location_latitude: String
                        , location_longitude: String
                        , value: String
                      )

}

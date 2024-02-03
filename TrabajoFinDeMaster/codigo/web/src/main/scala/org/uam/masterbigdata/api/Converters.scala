package org.uam.masterbigdata.api

import org.uam.masterbigdata.api.ApiModel.BuildInfoDto
import org.uam.masterbigdata.api.model.BuildInfo
import org.uam.masterbigdata.domain.AdapterModel.{EventView, FrameView, JourneyView, LocationView}
import org.uam.masterbigdata.domain.model.Entities.{Event, Frame, Journey}

trait Converters {
  import Converters._

  def buildInfoToApi(): BuildInfoDto = {
    BuildInfoDto(
      BuildInfo.name,
      BuildInfo.version
    )
  }

  def modelToApi(model:Journey): JourneyView = {
    JourneyView(
      model.id
      ,model.device_id
      ,model.start_timestamp
      ,model.start_location_address
      ,model.start_location_latitude
      ,model.start_location_longitude
      ,model.end_timestamp
      ,model.end_location_address
      ,model.end_location_latitude
      ,model.end_location_longitude
      ,model.distance
      ,model.consumption
      ,model.label
    )
  }

  def modelToApi(model: Event): EventView = {
    EventView(
      model.id
      , model.device_id
      , model.created
      , model.type_id
      , model.location_address
      , model.location_latitude
      , model.location_longitude
      , model.value
    )
  }


  def modelToApi(model: Frame): FrameView = {
    val locationView:LocationView = LocationView(
      model.location_created
      ,model.location_address
      ,model.location_latitude
      ,model.location_longitude
      ,model.location_altitude
      , model.location_speed
      ,model.location_valid
      ,model.location_course
    )

    FrameView(
      model.id
      , model.device_id
      , model.created
      , model.received
      , locationView
      ,model.ignition
    )
  }
}

object Converters extends Converters {

}

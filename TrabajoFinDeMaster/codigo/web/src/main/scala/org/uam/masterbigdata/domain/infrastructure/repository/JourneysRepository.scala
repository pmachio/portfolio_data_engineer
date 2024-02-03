package org.uam.masterbigdata.domain.infrastructure.repository

import org.uam.masterbigdata.domain.infrastructure.model.Entities.JourneyDbo
import slick.dbio.DBIO
trait JourneysRepository {
  def find(deviceId:Long):DBIO[Seq[JourneyDbo]]
  def findByLabel(deviceId:Long, label:String):DBIO[Seq[JourneyDbo]]
  def findById(deviceId:Long, id:String):DBIO[JourneyDbo]
}

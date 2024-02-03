package org.uam.masterbigdata.domain.infrastructure.repository

import org.uam.masterbigdata.domain.infrastructure.model.Entities.EventDbo
import slick.dbio.DBIO

trait EventsRepository {
  def find(deviceId: Long): DBIO[Seq[EventDbo]]

  def findById(deviceId: Long, id: String): DBIO[EventDbo]
}

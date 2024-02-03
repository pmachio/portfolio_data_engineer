package org.uam.masterbigdata.domain.infrastructure.repository

import org.uam.masterbigdata.domain.infrastructure.model.Entities.FrameDbo
import slick.dbio.DBIO

trait FramesRepository {
  def find(deviceId: Long): DBIO[Seq[FrameDbo]]

  def findById(deviceId: Long, id: Long): DBIO[FrameDbo]
}

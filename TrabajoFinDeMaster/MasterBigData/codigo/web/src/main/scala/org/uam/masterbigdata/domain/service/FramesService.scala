package org.uam.masterbigdata.domain.service

import org.uam.masterbigdata.domain.model.Entities.{Frame, FramesByDeviceIdAndFrameIdRequest, FramesByDeviceIdRequest}

import scala.concurrent.Future

trait FramesService {
  def getDeviceFrames(request: FramesByDeviceIdAndFrameIdRequest): Future[Frame]
  def getAllDeviceFrames(request: FramesByDeviceIdRequest): Future[Seq[Frame]]
}

object FramesService
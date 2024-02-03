package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.model.Entities
import org.uam.masterbigdata.domain.service.FramesService

import scala.concurrent.Future

class FramesServiceBase(model:ModelService[Future]) extends FramesService{
  override def getDeviceFrames(request: Entities.FramesByDeviceIdAndFrameIdRequest): Future[Entities.Frame] = model.findFrameById(request.deviceId, request.id)

  override def getAllDeviceFrames(request: Entities.FramesByDeviceIdRequest): Future[Seq[Entities.Frame]] = model.findAllFrames(request.deviceId)
}

object FramesServiceBase{
  def apply(model:ModelService[Future]) = new FramesServiceBase(model)
}

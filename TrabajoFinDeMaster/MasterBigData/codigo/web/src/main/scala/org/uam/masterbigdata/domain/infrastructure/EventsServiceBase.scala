package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.model.Entities
import org.uam.masterbigdata.domain.service.EventsService

import scala.concurrent.Future

class EventsServiceBase(modelService: ModelService[Future]) extends EventsService{
  override def getDeviceEvent(request: Entities.EventByDeviceIdAndEventIdRequest): Future[Entities.Event] = modelService.findEventById(request.deviceId, request.id)

  override def getAllDeviceEvents(request: Entities.EventsByDeviceIdRequest): Future[Seq[Entities.Event]] = modelService.findAllEvents(request.deviceId)
}
object EventsServiceBase{
  def apply(modelService: ModelService[Future]) = new EventsServiceBase(modelService)
}

package org.uam.masterbigdata.domain.service

import org.uam.masterbigdata.domain.model.Entities.{Event, EventByDeviceIdAndEventIdRequest, EventsByDeviceIdRequest}

import scala.concurrent.Future

trait EventsService{
    def getDeviceEvent(request:EventByDeviceIdAndEventIdRequest):Future[Event]
    def getAllDeviceEvents(request:EventsByDeviceIdRequest):Future[Seq[Event]]
}

object EventsService

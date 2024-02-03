package org.uam.masterbigdata.api.documentation

import org.uam.masterbigdata.domain.model.Entities.{EventByDeviceIdAndEventIdRequest, EventsByDeviceIdRequest, FramesByDeviceIdAndFrameIdRequest, FramesByDeviceIdRequest, JourneyByDeviceIdRequest, JourneysByDeviceIdAndLabelRequest, JourneysByDeviceIdRequest}

trait ApiMapper {
  //Journeys
  private[api] lazy val mapToDeviceJourneyRequest: ((Long, String)) => JourneyByDeviceIdRequest =
    request => JourneyByDeviceIdRequest(request._1, request._2)

  private[api] lazy val mapToDeviceJourneysByLabelRequest:  ((Long, String))  => JourneysByDeviceIdAndLabelRequest =
    request => JourneysByDeviceIdAndLabelRequest( request._1, request._2)


  private[api] lazy val mapToDeviceJourneysRequest: Long => JourneysByDeviceIdRequest =
    request => JourneysByDeviceIdRequest(request)

  //Events
  private[api] lazy val mapToDeviceEventRequest: ((Long, String)) => EventByDeviceIdAndEventIdRequest =
    request => EventByDeviceIdAndEventIdRequest(request._1, request._2)

  private[api] lazy val mapToDeviceEventsRequest: Long => EventsByDeviceIdRequest =
    request => EventsByDeviceIdRequest(request)

  //Frames
  private[api] lazy val mapToDeviceFrameRequest: ((Long, Long)) => FramesByDeviceIdAndFrameIdRequest =
    request => FramesByDeviceIdAndFrameIdRequest(request._1, request._2)

  private[api] lazy val mapToDeviceFramesRequest: Long => FramesByDeviceIdRequest =
    request => FramesByDeviceIdRequest(request)
}

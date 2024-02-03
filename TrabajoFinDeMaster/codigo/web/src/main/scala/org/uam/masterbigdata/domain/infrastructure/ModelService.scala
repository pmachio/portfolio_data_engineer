package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.model.Entities.{Event, Journey, Frame}

import scala.concurrent.Future
/**
 * Traits de los services
 * * @tparam F type constructor
 * */
trait ModelService[F[_]] {
  //Journeys
  def findAllJourneys(deviceId:Long):Future[Seq[Journey]]
  def findJourneysByLabel(deviceId:Long,label:String):Future[Seq[Journey]]
  def findJourneyById(deviceId:Long,id:String):Future[Journey]

  //Events
  def findAllEvents(deviceId: Long): Future[Seq[Event]]
  def findEventById(deviceId: Long, id: String): Future[Event]

  //Frame
  def findAllFrames(deviceId: Long): Future[Seq[Frame]]
  def findFrameById(deviceId: Long, id: Long): Future[Frame]
}

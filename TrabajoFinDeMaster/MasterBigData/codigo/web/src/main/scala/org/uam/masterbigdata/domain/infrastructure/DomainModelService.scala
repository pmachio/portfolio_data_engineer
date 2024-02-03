package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.domain.infrastructure.model.Entities.{JourneyDbo, EventDbo, FrameDbo}
import org.uam.masterbigdata.domain.model.Entities.{Journey, Event, Frame}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


final  class DomainModelService(dal: DataAccessLayer) extends ModelService[Future] with ComponentLogging {
  import dal.executeOperation
//Journeys
  override def findAllJourneys(deviceId: Long): Future[Seq[Journey]] = {
    log.info("Buscando todos los trayectos del device $deviceId")
    for {
      dbo <- dal.journeysRepository.find(deviceId)
    } yield dbo.map(dboToModel)
  }

  private def dboToModel(dbo: JourneyDbo): Journey =
    Journey(dbo.id, dbo.device_id.toString, dbo.start_timestamp.toString, dbo.start_location_address, dbo.start_location_latitude.toString
      , dbo.start_location_longitude.toString, dbo.end_timestamp.toString, dbo.end_location_address, dbo.end_location_latitude.toString
      , dbo.end_location_longitude.toString, dbo.distance.toString, dbo.consumption.toString, dbo.label)

  override def findJourneysByLabel(deviceId: Long,label: String): Future[Seq[Journey]] = {
    log.info(s"Buscando todos los trayectos por label $label del device $deviceId")
    for {
      dbo <- dal.journeysRepository.findByLabel(deviceId, label)
    } yield dbo.map(dboToModel)
  }

  override def findJourneyById(deviceId: Long, id:String): Future[Journey] = {
    log.info(s"Buscando todos los trayectos por id $id del device $deviceId")
    for {
      dbo <- dal.journeysRepository.findById(deviceId, id)
    } yield dboToModel(dbo)
  }



  //Events
  private def dboToModel(dbo: EventDbo): Event =
    Event(dbo.id.toString, dbo.device_id.toString, dbo.created.toString, dbo.type_id.toString, dbo.location_address.getOrElse("")
      , dbo.location_latitude match {
        case Some(lat) => lat.toString
        case None => ""
      }, dbo.location_longitude
      match {
        case Some(lon) => lon.toString
        case None => ""
      }, dbo.value)
  override def findAllEvents(deviceId: Long): Future[Seq[Event]] =
      for{
          dbo <- dal.eventsRepository.find(deviceId)
      }yield dbo.map(dboToModel)

  override def findEventById(deviceId: Long, id: String): Future[Event] =
    for{
      dbo <- dal.eventsRepository.findById(deviceId, id)
    }yield dboToModel(dbo)

  //Frames
  private def dboToModel(dbo: FrameDbo): Frame =
    Frame(dbo.id.toString, dbo.device_id.toString, dbo.created.toString, dbo.received.toString, dbo.location_created.toString
    ,dbo.location_address, dbo.location_latitude.toString, dbo.location_longitude.toString, dbo.location_altitude.toString
      , dbo.location_speed.toString, dbo.location_valid.toString, dbo.location_course.toString, dbo.ignition.toString)
  override def findAllFrames(deviceId: Long): Future[Seq[Frame]] = for{
    dbo <- dal.framesRepository.find(deviceId)
  }yield dbo.map(dboToModel)

  override def findFrameById(deviceId: Long, id: Long): Future[Frame] = for{
    dbo <- dal.framesRepository.findById(deviceId, id)
  }yield dboToModel(dbo)
}

object DomainModelService {
  def apply(dal: DataAccessLayer) = new DomainModelService(dal)
}
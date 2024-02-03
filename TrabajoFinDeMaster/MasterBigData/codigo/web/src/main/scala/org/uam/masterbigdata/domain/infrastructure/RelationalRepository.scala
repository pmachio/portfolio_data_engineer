package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.infrastructure.model.Entities.{EventDbo, FrameDbo, JourneyDbo}
import org.uam.masterbigdata.ComponentLogging
import org.uam.masterbigdata.domain.infrastructure.repository.{EventsRepository, FramesRepository, JourneysRepository}
import slick.ast.ColumnOption.PrimaryKey
import slick.dbio
import slick.lifted.ProvenShape

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global

trait RelationalRepository extends ComponentLogging  {
  self: Profile =>

  import profile.api._

  /**Table*/
  private val journeyQueryTable = TableQuery[JourneyTable]
  private val eventQueryTable = TableQuery[EventTable]
  private val frameQueryTable = TableQuery[FrameTable]

  /**
   * Journey
   * */
  //M A P P I N G * F U N C T I O N
  private def intoJourney(row: (String, Long,Timestamp,String,Float,Float, Timestamp, String, Float,Float, Long, Long, String)): JourneyDbo =
    JourneyDbo(row._1, row._2, row._3.getTime, row._4, row._5, row._6, row._7.getTime, row._8, row._9, row._10, row._11, row._12, row._13)
  private def fromJourney(journey: JourneyDbo):Option[(String, Long,Timestamp,String,Float,Float, Timestamp, String, Float,Float, Long, Long, String)] =
    Some((journey.id, journey.device_id, new Timestamp(journey.start_timestamp), journey.start_location_address
      , journey.start_location_latitude, journey.start_location_longitude, new Timestamp(journey.end_timestamp)
    ,journey.end_location_address, journey.end_location_latitude, journey.end_location_longitude, journey.distance, journey.consumption, journey.label))
  //Descripción de la tabla
  final class JourneyTable(tag: Tag)extends Table[JourneyDbo](tag, "journeys") {
    override def * : ProvenShape[JourneyDbo] = (id, device_id, start_timestamp, start_location_address
      , start_location_latitude,start_location_longitude, end_timestamp
      , end_location_address, end_location_latitude, end_location_longitude, distance, consumption, label.getOrElse(""))<> (intoJourney, fromJourney)
    def id = column[String]("id", PrimaryKey)
    def device_id =column[ Long]("device_id")
    def start_timestamp=column[Timestamp]("start_timestamp")
    def start_location_address=column[String]("start_location_address")
    def start_location_latitude=column[Float]("start_location_latitude")
    def start_location_longitude=column[Float]("start_location_longitude")
    def end_timestamp=column[Timestamp]("end_timestamp")
    def end_location_address=column[String]("end_location_address")
    def end_location_latitude=column[Float]("end_location_latitude")
    def end_location_longitude=column[Float]("end_location_longitude")
    def distance=column[Long]("distance")
    def consumption=column[Long]("consumption")
    def label= column[Option[String]]("label")
  }
  //funciones
  final class JourneysRelationalRepository extends JourneysRepository {
    lazy val entities = journeyQueryTable
    override def find(deviceId: Long): dbio.DBIO[Seq[JourneyDbo]] =
      entities.filter(_.device_id === deviceId).result
    override def findByLabel(deviceId: Long, label: String): dbio.DBIO[Seq[JourneyDbo]] =
      entities.filter(_.device_id === deviceId).filter(_.label === label).result
    override def findById(deviceId: Long, id: String): dbio.DBIO[JourneyDbo] = {
      val search = entities.filter(_.device_id === deviceId).filter(_.id === id)
      search.result.flatMap(xs => xs.length match {
        case 0 => DBIO.failed(new RuntimeException(s"No existe el trayecto $id para el dispositivo $deviceId"))
        case 1 => DBIO.successful(xs.head)
        case _ => DBIO.failed(new RuntimeException(s"Existen múltiples trayectos para el $id y dispositivo $deviceId"))
      })
    }
  }

  /**
   * Events
   * */
  private def intoEvent(row: (String,Long,Timestamp,Long,Option[String],Option[Float],Option[Float],String)): EventDbo =
    EventDbo(row._1, row._2, row._3.getTime, row._4, row._5, row._6, row._7, row._8)
  private def fromEvent(event: EventDbo): Option[(String, Long,Timestamp,Long,Option[String],Option[Float],Option[Float],String)] =
    Some((event.id, event.device_id, new Timestamp(event.created), event.type_id, event.location_address, event.location_latitude, event.location_longitude, event.value))
  final class EventTable(tag: Tag)extends Table[EventDbo](tag, "events") {
    override def * : ProvenShape[EventDbo] = (id, device_id, created, type_id, location_address, location_latitude
    ,location_longitude, value) <> (intoEvent, fromEvent)

    def  id = column[String]("id", PrimaryKey)
    def device_id= column[Long]("device_id")
    def created= column[ Timestamp]("created")
    def type_id= column[ Long]("type_id")
    def location_address= column[Option[String]]("location_address")
    def location_latitude= column[Option[Float]]("location_latitude")
    def location_longitude= column[Option[Float]]("location_longitude")
    def value= column[ String]("value")
  }

  final class EventsRelationalRepository extends EventsRepository {
    lazy val entities = eventQueryTable

    override def find(deviceId: Long): dbio.DBIO[Seq[EventDbo]] =
      entities.filter(_.device_id === deviceId).result

    override def findById(deviceId: Long, id: String): dbio.DBIO[EventDbo] = {
      val search = entities.filter(_.device_id === deviceId).filter(_.id === id)
      search.result.flatMap(xs => xs.length match {
        case 0 => DBIO.failed(new RuntimeException(s"No existe el trayecto $id para el dispositivo $deviceId"))
        case 1 => DBIO.successful(xs.head)
        case _ => DBIO.failed(new RuntimeException(s"Existen múltiples trayectos para el $id y dispositivo $deviceId"))
      })
    }
  }
    /**
     * Frames
     * */
    private def intoFrames(row: (Long, Long, Timestamp, Timestamp, Timestamp, String, Float, Float, Float, Float, Boolean, Float, Boolean)): FrameDbo =
      FrameDbo(row._1, row._2, row._3.getTime, row._4.getTime, row._5.getTime, row._6, row._7, row._8, row._9, row._10, row._11, row._12, row._13)

    private def fromFrames(frame: FrameDbo): Option[(Long, Long, Timestamp, Timestamp, Timestamp, String, Float, Float, Float, Float, Boolean, Float, Boolean)] =
      Some((frame.id, frame.device_id, new Timestamp(frame.created), new Timestamp(frame.received), new Timestamp(frame.location_created)
        , frame.location_address, frame.location_latitude, frame.location_longitude, frame.location_altitude, frame.location_speed
        , frame.location_valid, frame.location_course, frame.ignition))
    final class FrameTable(tag: Tag) extends Table[FrameDbo](tag, "frames") {
      override def * : ProvenShape[FrameDbo] = (id, device_id, created, received, location_created, location_address
        , location_latitude, location_longitude, location_altitude, location_speed, location_valid, location_course, ignition) <> (intoFrames, fromFrames)

      def id = column[Long]("id")

      def device_id = column[Long]("device_id")

      def created = column[Timestamp]("created")

      def received = column[Timestamp]("received")

      def location_created = column[Timestamp]("location_created")

      def location_address = column[String]("location_address")

      def location_latitude = column[Float]("location_latitude")

      def location_longitude = column[Float]("location_longitude")

      def location_altitude = column[Float]("location_altitude")

      def location_speed = column[Float]("location_speed")

      def location_valid = column[Boolean]("location_valid")

      def location_course = column[Float]("location_course")

      def ignition = column[Boolean]("ignition")
    }

    final class FramesRelationalRepository extends FramesRepository {
      lazy val entities = frameQueryTable

      override def find(deviceId: Long): dbio.DBIO[Seq[FrameDbo]] =
        entities.filter(_.device_id === deviceId).result

      override def findById(deviceId: Long, id: Long): dbio.DBIO[FrameDbo] = {
        val search = entities.filter(_.device_id === deviceId).filter(_.id === id)
        search.result.flatMap(xs => xs.length match {
          case 0 => DBIO.failed(new RuntimeException(s"No existe el frame $id para el dispositivo $deviceId"))
          case 1 => DBIO.successful(xs.head)
          case _ => DBIO.failed(new RuntimeException(s"Existen múltiples frames para el $id y dispositivo $deviceId"))
        })
      }
    }

}

package org.uam.masterbigdata.api.documentation

import io.circe._
//import io.circe.parser.parse
import io.circe.syntax._

import org.uam.masterbigdata.domain.AdapterModel.{JourneyView, EventView, FrameView, LocationView}
import org.uam.masterbigdata.domain.model.error.DomainError._
import org.uam.masterbigdata.domain.model.error.DomainException

import sttp.tapir.Codec.JsonCodec
import sttp.tapir.json.circe._

//import scala.collection.mutable


trait JsonCodecs {


  //Journeys
  private[api] implicit lazy val journeysViewCodec: JsonCodec[JourneyView] =
    implicitly[JsonCodec[Json]].map(json => json.as[JourneyView] match {
      case Left(_) => throw DomainException(MessageParsingError)
      case Right(value) => value
    })(coordinates => coordinates.asJson)

  private[api] implicit lazy val decodeJourneysView: Decoder[JourneyView] = (c: HCursor) => for {
    id <- c.get[String]("id")
    device_id <- c.get[String]("device_id")
    start_timestamp <- c.get[String]("start_timestamp")
    start_location_address <- c.get[String]("start_location_address")
    start_location_latitude <- c.get[String]("start_location_latitude")
    start_location_longitude <- c.get[String]("start_location_longitude")
    end_timestamp <- c.get[String]("end_timestamp")
    end_location_address <- c.get[String]("end_location_address")
    end_location_latitude <- c.get[String]("end_location_latitude")
    end_location_longitude <- c.get[String]("end_location_longitude")
    distance <- c.get[String]("distance")
    consumption <- c.get[String]("consumption")
    label <- c.get[String]("label")
  } yield JourneyView(id, device_id, start_timestamp, start_location_address, start_location_latitude, start_location_longitude, end_timestamp, end_location_address, end_location_latitude, end_location_longitude, distance, consumption, label)

  private[api] implicit lazy val encodeJourneysView: Encoder[JourneyView] = (a: JourneyView) => {
    Json.obj(
      ("id", a.id.asJson),
      ("device_id", a.device_id.asJson),
      ("start_timestamp", a.start_timestamp.asJson),
      ("start_location_address", a.start_location_address.asJson),
      ("start_location_latitude", a.start_location_latitude.asJson),
      ("start_location_longitude", a.start_location_longitude.asJson),
      ("end_timestamp", a.end_timestamp.asJson),
      ("end_location_address", a.end_location_address.asJson),
      ("end_location_latitude", a.end_location_latitude.asJson),
      ("end_location_longitude", a.end_location_longitude.asJson),
      ("distance", a.distance.asJson),
      ("consumption", a.consumption.asJson),
      ("label", a.label.asJson)
    )
  }

  private[api] implicit lazy val seqJourneysViewCodec: JsonCodec[Seq[JourneyView]] =
    implicitly[JsonCodec[Json]].map(json => json.as[Seq[JourneyView]](io.circe.Decoder.decodeSeq[JourneyView](decodeJourneysView)) match {
      case Left(_) =>
        throw DomainException(MessageParsingError)
      case Right(value) => value
    })(assetShape => assetShape.asJson(io.circe.Encoder.encodeSeq[JourneyView](encodeJourneysView)))


  //Events
  private[api] implicit lazy val eventViewCodec: JsonCodec[EventView] =
    implicitly[JsonCodec[Json]].map(json => json.as[EventView] match {
      case Left(_) => throw DomainException(MessageParsingError)
      case Right(value) => value
    })(coordinates => coordinates.asJson)

  private[api] implicit lazy val decodeEventView: Decoder[EventView] = (c: HCursor) => for {
    id <- c.get[String]("id")
    device_id <- c.get[String]("device_id")
    created <- c.get[String]("created")
    type_id <- c.get[String]("type_id")
    location_address <- c.get[String]("location_address")
    location_latitude <- c.get[String]("location_latitude")
    location_longitude <- c.get[String]("location_longitude")
    value <- c.get[String]("value")
  } yield EventView(id, device_id, created, type_id, location_address, location_latitude, location_longitude, value)

  private[api] implicit lazy val encodeEventView: Encoder[EventView] = (a: EventView) => {
    Json.obj(
      ("id", a.id.asJson),
      ("device_id", a.device_id.asJson),
      ("created", a.created.asJson),
      ("type_id", a.type_id.asJson),
      ("location_address", a.location_address.asJson),
      ("location_latitude", a.location_latitude.asJson),
      ("location_longitude", a.location_longitude.asJson),
      ("value", a.value.asJson)
    )
  }

  private[api] implicit lazy val seqEventsViewCodec: JsonCodec[Seq[EventView]] =
    implicitly[JsonCodec[Json]].map(json => json.as[Seq[EventView]](io.circe.Decoder.decodeSeq[EventView](decodeEventView)) match {
      case Left(_) =>
        throw DomainException(MessageParsingError)
      case Right(value) => value
    })(assetShape => assetShape.asJson(io.circe.Encoder.encodeSeq[EventView](encodeEventView)))

  //Location
  private[api] implicit lazy val locationViewCodec: JsonCodec[LocationView] =
    implicitly[JsonCodec[Json]].map(json => json.as[LocationView] match {
      case Left(_) => throw DomainException(MessageParsingError)
      case Right(value) => value
    })(coordinates => coordinates.asJson)

  private[api] implicit lazy val decodeLocationView: Decoder[LocationView] = (c: HCursor) => for {
    created <- c.get[String]("created")
    address <- c.get[String]("address")
    latitude <- c.get[String]("latitude")
    longitude <- c.get[String]("longitude")
    altitude <- c.get[String]("altitude")
    speed <- c.get[String]("speed")
    valid <- c.get[String]("valid")
    course <- c.get[String]("course")
  } yield LocationView(created, address, latitude, longitude, altitude, speed, valid, course)

  private[api] implicit lazy val encodeLocationView: Encoder[LocationView] = (a: LocationView) => {
    Json.obj(
      ("created", a.created.asJson),
      ("address", a.address.asJson),
      ("latitude", a.latitude.asJson),
      ("longitude", a.longitude.asJson),
      ("altitude", a.altitude.asJson),
      ("speed", a.speed.asJson),
      ("valid", a.valid.asJson),
      ("course", a.course.asJson)
    )
  }

  //Frames
  private[api] implicit lazy val frameViewCodec: JsonCodec[FrameView] =
    implicitly[JsonCodec[Json]].map(json => json.as[FrameView] match {
      case Left(_) => throw DomainException(MessageParsingError)
      case Right(value) => value
    })(coordinates => coordinates.asJson)

  private[api] implicit lazy val decodeFrameView: Decoder[FrameView] = (c: HCursor) => for {
    id <- c.get[String]("id")
    device_id <- c.get[String]("device_id")
    created <- c.get[String]("created")
    received <- c.get[String]("received")
    location <- c.get[LocationView]("location")
    ignition <- c.get[String]("ignition")
  } yield FrameView(id, device_id, created, received, location, ignition)

  private[api] implicit lazy val encodeFrameView: Encoder[FrameView] = (a: FrameView) => {
    Json.obj(
      ("id", a.id.asJson),
      ("device_id", a.device_id.asJson),
      ("created", a.created.asJson),
      ("received", a.received.asJson),
      ("location", a.location.asJson),
      ("ignition", a.ignition.asJson)
    )
  }

  private[api] implicit lazy val seqFramesViewCodec: JsonCodec[Seq[FrameView]] =
    implicitly[JsonCodec[Json]].map(json => json.as[Seq[FrameView]](io.circe.Decoder.decodeSeq[FrameView](decodeFrameView)) match {
      case Left(_) =>
        throw DomainException(MessageParsingError)
      case Right(value) => value
    })(assetShape => assetShape.asJson(io.circe.Encoder.encodeSeq[FrameView](encodeFrameView)))

}

object JsonCodecs extends JsonCodecs

package org.uam.masterbigdata.domain.infrastructure

import slick.dbio
import slick.basic.{DatabaseConfig, DatabasePublisher}
import slick.jdbc.JdbcProfile

trait Entity[T, I] {
  val id: Option[I]
}

/** Abstract profile for accessing SQL databases via JDBC. */
trait Profile {
  /*
    Even when using a standard interface for database drivers like JDBC
    there are many differences between databases in the SQL dialect they
    understand, the way they encode data types, or other idiosyncracies.
    Slick abstracts over these differences with profiles.
   */
  val profile: JdbcProfile

}

/**
 * Provides database configuration to the user component
 */
trait DbConfigModule {
  val databaseConfig: DatabaseConfig[JdbcProfile]
}

/**
 * Provides database execution context to the user component.
 * Side effect for pure DBIO's.
 * Interprets the DBIO program sentences.
 */
trait DbModule
  extends Profile {

  import slick.dbio.{DBIO, StreamingDBIO}

  import scala.concurrent.Future

  val db: JdbcProfile#Backend#Database

  implicit def executeOperation[T](dbio: DBIO[T]): Future[T] = {
    db.run(dbio)
  }

  implicit def dbioToDbPub[T](dbio: StreamingDBIO[Seq[T], T]): DatabasePublisher[T] = {
    db.stream(dbio)
  }
}
package org.uam.masterbigdata.domain.infrastructure

import org.uam.masterbigdata.domain.infrastructure.repository.{EventsRepository, FramesRepository, JourneysRepository}

trait PersistenceModule {
  val journeysRepository : JourneysRepository
  val eventsRepository: EventsRepository
  val framesRepository: FramesRepository
}

trait DataAccessLayer
  extends DbConfigModule
    with DbModule
    with PersistenceModule

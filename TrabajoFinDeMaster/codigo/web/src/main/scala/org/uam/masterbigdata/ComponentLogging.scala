package org.uam.masterbigdata

import org.slf4j.{Logger, LoggerFactory}

/**
 * Proprociona un logger al componente que lo utilice (mixin)
 */
trait ComponentLogging {
  val log: Logger = LoggerFactory.getLogger(getClass)
}

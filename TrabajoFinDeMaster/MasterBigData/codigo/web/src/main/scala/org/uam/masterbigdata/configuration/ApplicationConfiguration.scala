package org.uam.masterbigdata.configuration
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteConcatenation._
import akka.stream.Materializer
import com.typesafe.config.{Config, ConfigFactory}
import org.uam.masterbigdata.api.{ActuatorApi, EventsApi, FramesApi, JourneysApi, SwaggerApi}
import org.uam.masterbigdata.domain.infrastructure.repository.{EventsRepository, FramesRepository}
import org.uam.masterbigdata.domain.infrastructure.{DataAccessLayer, DomainModelService, EventsServiceBase, FramesServiceBase, JourneyServiceBase, RelationalRepository}
import org.uam.masterbigdata.domain.service.{EventsService, FramesService, JourneysService}
import slick.basic.DatabaseConfig
import slick.jdbc.{JdbcBackend, JdbcProfile}

trait ApplicationConfiguration {
  //AKKA
  val system: ActorSystem
  implicit val materializer: Materializer

  private val config: Config = ConfigFactory.load()

  //Propio servidor web
  private val serverPath = "application.server"

  val serverAddress: String = config.getString(s"$serverPath.interface")
  val serverPort: Int = config.getInt(s"$serverPath.port")

  /**INFRAESTRUCTURA**/
  // I N F R A S T R U C T U R A
  val dataAccessLayer: DataAccessLayer = {
    new DataAccessLayer
      with RelationalRepository {

      override val databaseConfig: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig[JdbcProfile]("infrastructure.postgres")
      override val db: JdbcBackend#DatabaseDef = databaseConfig.db
      override val profile: JdbcProfile = slick.jdbc.PostgresProfile
      override val journeysRepository = new JourneysRelationalRepository()
      override val eventsRepository: EventsRepository = new EventsRelationalRepository()
      override val framesRepository: FramesRepository = new FramesRelationalRepository()
    }
  }
  val modeler = new DomainModelService(dataAccessLayer)
  /**SERVICIOS**/

  //Journeys
  private val journeysService:JourneysService = JourneyServiceBase(modeler)
  private val journeysApi: JourneysApi = JourneysApi(journeysService)
  //Events
  private val eventsService:EventsService = EventsServiceBase(modeler)
  private val eventsApi: EventsApi = EventsApi(eventsService)
  //Frames
  private val framesService: FramesService = FramesServiceBase(modeler)
  private val framesApi: FramesApi = FramesApi(framesService)


  /**RUTAS WEB*/
  lazy val routes: Route =
    ActuatorApi.route ~ SwaggerApi.route ~ journeysApi.routes ~ eventsApi.routes ~framesApi.routes
}

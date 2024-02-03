package org.uam.masterbigdata.api.documentation

import org.uam.masterbigdata.api.ApiModel.BuildInfoDto
import org.uam.masterbigdata.api.Converters
import org.uam.masterbigdata.api.documentation.ApiEndpoint._

import io.circe.generic.auto._
import sttp.model.StatusCode
import sttp.tapir.json.circe._
import sttp.tapir.{Endpoint, jsonBody, _}

trait ActuatorEndpoint {

  type HealthInfo = BuildInfoDto
  private[api] lazy val healthResource: String = "health"
  private[api] lazy val healthNameResource: String = "health-resource"
  private[api] lazy val healthDescriptionResource: String = "Asset Composer Service Health Check Endpoint"

  // E N D P O I N T
  private[api] lazy val healthEndpoint: Endpoint[Unit, StatusCode, HealthInfo, Nothing] =
    baseEndpoint
      .get
      .in(healthResource)
      .name(healthNameResource)
      .description(healthDescriptionResource)
      .out(jsonBody[HealthInfo].example(Converters.buildInfoToApi()))
      .errorOut(statusCode).tag(healthResource)


}

object ActuatorEndpoint extends ActuatorEndpoint

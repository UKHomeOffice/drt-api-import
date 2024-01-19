package advancepassengerinfo.health

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

import scala.concurrent.duration.DurationInt

object HealthRoute {

  def checkHealth(healthCheckedState: HealthCheckedState): Route =
    get {
      if (healthCheckedState.hasCheckedSince(5.minutes))
        complete(StatusCodes.OK)
      else
        complete(StatusCodes.InternalServerError, "KO")
    }

  def apply(healthCheckedState: HealthCheckedState): Route =
    pathPrefix("health-check") {
      checkHealth(healthCheckedState)
    }
}


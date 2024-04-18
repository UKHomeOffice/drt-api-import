package advancepassengerinfo.health

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object HealthRoute {

  def isHealthy(lastCheckedState: LastCheckedState, threshold: FiniteDuration): Route =
    get {
      if (lastCheckedState.hasCheckedSince(threshold))
        complete(StatusCodes.OK)
      else
        complete(StatusCodes.InternalServerError, "KO")
    }

  def apply(lastCheckedState: LastCheckedState, threshold: FiniteDuration): Route =
    pathPrefix("health-check") {
      isHealthy(lastCheckedState, threshold)
    }
}


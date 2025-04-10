package advancepassengerinfo.health

import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route

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


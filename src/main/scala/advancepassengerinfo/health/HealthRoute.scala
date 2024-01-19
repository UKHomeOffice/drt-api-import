package advancepassengerinfo.health

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

object HealthRoute {

  def checkHealth(processState: ProcessState): Route =
    get {
      if (processState.hasCheckedSince)
        complete(StatusCodes.OK)
      else
        complete(StatusCodes.InternalServerError, "KO")
    }

  def apply(processState: ProcessState): Route =
    pathPrefix("health-check") {
      checkHealth(processState)
    }
}


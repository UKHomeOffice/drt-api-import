package advancepassengerinfo.health

import akka.http.scaladsl.model.StatusCodes
import scala.concurrent.ExecutionContext
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

object HealthRoute {

  def checkHealth(processState: ProcessState)(implicit ec: ExecutionContext): Route =
    get {
      if (processState.isLatest)
        complete(StatusCodes.OK)
      else
        complete((StatusCodes.InternalServerError, "KO"))
    }

  def apply(processState: ProcessState)(implicit ec: ExecutionContext): Route =
    pathPrefix("health-check") {
      checkHealth(processState)
    }
}


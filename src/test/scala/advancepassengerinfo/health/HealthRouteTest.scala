package advancepassengerinfo.health

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import drtlib.SDate
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{DurationInt, FiniteDuration}


class HealthRouteTest extends AnyFlatSpec with Matchers with ScalatestRouteTest {

  private val threshold: FiniteDuration = 5.minutes

  private val now: SDate = SDate("2021-01-01T00:00:00")

  "HealthRoute" should "return InternalServerError status if last checked is not updated" in {

    val route = HealthRoute(LastCheckedState(() => now), threshold)

    Get("/health-check") ~> route ~> check {
      status shouldBe StatusCodes.InternalServerError
      responseAs[String] shouldBe "KO"
    }
  }

  it should "return OK status if last checked is within 5 minutes" in {

    val lastCheckedState = LastCheckedState(() => now)

    lastCheckedState.setLastCheckedAt(now.minus(threshold.minus(1.second)))

    val route = HealthRoute(lastCheckedState, threshold)

    Get("/health-check") ~> route ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  it should "return InternalServerError status if last checked is not with 5 minutes" in {

    val lastCheckedState = LastCheckedState(() => now)

    lastCheckedState.setLastCheckedAt(now.minus(threshold.plus(1.second)))

    val route = HealthRoute(lastCheckedState, threshold)

    Get("/health-check") ~> route ~> check {
      status shouldBe StatusCodes.InternalServerError
      responseAs[String] shouldBe "KO"
    }
  }
}

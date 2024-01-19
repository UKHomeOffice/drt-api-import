package advancepassengerinfo.health

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant


class HealthRouteTest extends AnyFlatSpec with Matchers with ScalatestRouteTest {

  "HealthRoute" should "return InternalServerError status if last checked is not updated" in {

    val route = HealthRoute(LastCheckedState())

    Get("/health-check") ~> route ~> check {
      status shouldBe StatusCodes.InternalServerError
      responseAs[String] shouldBe "KO"
    }
  }

  it should "return OK status if last checked is within 5 minutes" in {

    val lastCheckedState = LastCheckedState()

    lastCheckedState.setLastCheckedAt(Instant.now().minusSeconds(299))

    val route = HealthRoute(lastCheckedState)

    Get("/health-check") ~> route ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  it should "return InternalServerError status if last checked is not with 5 minutes" in {

    val lastCheckedState = LastCheckedState()

    lastCheckedState.setLastCheckedAt(Instant.now().minusSeconds(300))

    val route = HealthRoute(lastCheckedState)

    Get("/health-check") ~> route ~> check {
      status shouldBe StatusCodes.InternalServerError
      responseAs[String] shouldBe "KO"
    }
  }
}

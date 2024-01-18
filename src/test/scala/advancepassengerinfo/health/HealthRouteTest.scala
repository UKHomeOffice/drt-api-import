package advancepassengerinfo.health

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HealthRouteTest extends AnyFlatSpec with Matchers with ScalatestRouteTest {

  "HealthRoute" should "return OK status if process state is latest" in {
    val mockProcessState = new ProcessState {
      override def isLatest: Boolean = true
    }

    val route = HealthRoute(mockProcessState)

    Get("/health-check") ~> route ~> check {
      status shouldBe StatusCodes.OK
    }
  }

  it should "return InternalServerError status if process state is not latest" in {
    val mockProcessState = new ProcessState {
      override def isLatest: Boolean = false
    }

    val route = HealthRoute(mockProcessState)

    Get("/health-check") ~> route ~> check {
      status shouldBe StatusCodes.InternalServerError
      responseAs[String] shouldBe "KO"
    }
  }
}

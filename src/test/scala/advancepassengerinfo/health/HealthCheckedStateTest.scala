package advancepassengerinfo.health

import java.time.Instant
import java.time.temporal.ChronoUnit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class HealthCheckedStateTest extends AnyFlatSpec with Matchers {

  "HealthCheckedState" should "return true for hasCheckedSince, if the lastCheckedAt is within the last 5 minutes" in {
    val processState = HealthCheckedState()
    processState.hasCheckedSince(5.minutes) shouldBe true
  }

  it should "return false for hasCheckedSince, if the lastCheckedAt is more than 5 minutes ago" in {
    val processState = HealthCheckedState()
    val sixMinutesAgo = Instant.now().minus(6, ChronoUnit.MINUTES)
    processState.setLastCheckedAt(sixMinutesAgo)
    processState.hasCheckedSince(5.minutes) shouldBe false
  }

}

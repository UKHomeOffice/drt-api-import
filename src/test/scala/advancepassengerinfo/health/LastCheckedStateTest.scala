package advancepassengerinfo.health

import java.time.Instant
import java.time.temporal.ChronoUnit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class LastCheckedStateTest extends AnyFlatSpec with Matchers {

  "LastCheckedState" should "return true for hasCheckedSince, if the lastCheckedAt is within the last 5 minutes" in {
    val lastCheckedState = LastCheckedState()
    lastCheckedState.hasCheckedSince(5.minutes) shouldBe true
  }

  it should "return false for hasCheckedSince, if the lastCheckedAt is more than 5 minutes ago" in {
    val lastCheckedState = LastCheckedState()
    val sixMinutesAgo = Instant.now().minus(6, ChronoUnit.MINUTES)
    lastCheckedState.setLastCheckedAt(sixMinutesAgo)
    lastCheckedState.hasCheckedSince(5.minutes) shouldBe false
  }

}

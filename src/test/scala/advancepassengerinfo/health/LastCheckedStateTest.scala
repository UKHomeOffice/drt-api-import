package advancepassengerinfo.health

import java.time.Instant
import java.time.temporal.ChronoUnit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class LastCheckedStateTest extends AnyFlatSpec with Matchers {

  "LastCheckedState" should "return false for hasCheckedSince, if the lastCheckedAt is not updated" in {
    val lastCheckedState = LastCheckedState()
    lastCheckedState.hasCheckedSince(5.minutes) shouldBe false
  }

  it should "return true for hasCheckedSince, if the lastCheckedAt is within 5 minutes ago" in {
    val lastCheckedState = LastCheckedState()
    val fourMinutesAgo = Instant.now().minus(4, ChronoUnit.MINUTES)
    lastCheckedState.setLastCheckedAt(fourMinutesAgo)
    lastCheckedState.hasCheckedSince(5.minutes) shouldBe true
  }

  it should "return false for hasCheckedSince, if the lastCheckedAt is more than 5 minutes ago" in {
    val lastCheckedState = LastCheckedState()
    val sixMinutesAgo = Instant.now().minus(6, ChronoUnit.MINUTES)
    lastCheckedState.setLastCheckedAt(sixMinutesAgo)
    lastCheckedState.hasCheckedSince(5.minutes) shouldBe false
  }

}

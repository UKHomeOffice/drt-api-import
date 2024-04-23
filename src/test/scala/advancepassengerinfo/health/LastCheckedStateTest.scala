package advancepassengerinfo.health

import drtlib.SDate
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class LastCheckedStateTest extends AnyFlatSpec with Matchers {

  private val checkThreshold = 5.minutes

  private val nowSdate: SDate = SDate("2021-01-01T00:00")

  "LastCheckedState" should "return false for hasCheckedSince, if the lastCheckedAt has not been set" in {
    val lastCheckedState = LastCheckedState(() => nowSdate)
    lastCheckedState.hasCheckedSince(checkThreshold) shouldBe false
  }

  it should "return true for hasCheckedSince, if the lastCheckedAt is within 5 minutes ago" in {
    val lastCheckedState = LastCheckedState(() => nowSdate)
    val underThreshold = nowSdate.minus(checkThreshold.minus(1.second))
    lastCheckedState.setLastCheckedAt(underThreshold)
    lastCheckedState.hasCheckedSince(checkThreshold) shouldBe true
  }

  it should "return false for hasCheckedSince, if the lastCheckedAt is more than 5 minutes ago" in {
    val lastCheckedState = LastCheckedState(() => nowSdate)
    val overThreshold = nowSdate.minus(checkThreshold.plus(1.second))
    lastCheckedState.setLastCheckedAt(overThreshold)
    lastCheckedState.hasCheckedSince(checkThreshold) shouldBe false
  }

}

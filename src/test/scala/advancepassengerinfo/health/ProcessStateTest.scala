package advancepassengerinfo.health

import java.time.Instant
import java.time.temporal.ChronoUnit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ProcessStateTest extends AnyFlatSpec with Matchers {

  "ProcessState" should "return true for isLatest if the state is within the last hour" in {
    val processState = ProcessState()
    processState.isLatest shouldBe true
  }

  it should "return false for isLatest if the state is more than an hour ago" in {
    val processState = ProcessState()
    val oneHourAndOneMinuteAgo = Instant.now().minus(61, ChronoUnit.MINUTES)
    processState.healthState = oneHourAndOneMinuteAgo
    processState.isLatest shouldBe false
  }

  it should "update the state to the current time" in {
    val processState = ProcessState()
    val beforeUpdate = Instant.now()
    val oneSecondInMillis = 1000
    Thread.sleep(oneSecondInMillis)
    processState.update()
    val afterUpdate = processState.healthState
    afterUpdate should be > beforeUpdate
  }
}

package advancepassengerinfo.health

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration

case class HealthCheckedState() {
  var lastCheckedAt: Instant = Instant.now()

  def hasCheckedSince(durationInMin: FiniteDuration): Boolean = {
    Duration.between(lastCheckedAt, Instant.now()).compareTo(Duration.ofMillis(durationInMin.toMillis)) < 0
  }

  def setLastCheckedAt(at:Instant): Unit = {
    lastCheckedAt = at
  }
}

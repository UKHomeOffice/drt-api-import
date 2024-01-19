package advancepassengerinfo.health

import java.time.{Duration, Instant}
import scala.concurrent.duration.FiniteDuration

case class LastCheckedState() {
  var lastCheckedAt: Option[Instant] = None

  def hasCheckedSince(durationInMin: FiniteDuration): Boolean = {
    lastCheckedAt.exists(lca => Duration.between(lca, Instant.now()).compareTo(Duration.ofMillis(durationInMin.toMillis)) < 0)
  }

  def setLastCheckedAt(at:Instant): Unit = {
    lastCheckedAt = Some(at)
  }
}

package advancepassengerinfo.health

import drtlib.SDate
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationLong, FiniteDuration}

case class LastCheckedState(now: () => SDate) {
  private val log = LoggerFactory.getLogger(getClass)
  private var lastCheckedAt: Option[SDate] = None

  def hasCheckedSince(threshold: FiniteDuration): Boolean =
    lastCheckedAt
      .exists { lca =>
        val lastCheckedAgo = (now().millisSinceEpoch - lca.millisSinceEpoch).millis

        val checkedWithinThreshold = lastCheckedAgo <= threshold

        if (!checkedWithinThreshold)
          log.warn(s"Last checked at $lca, which was $lastCheckedAgo ago, which is more than the threshold of $threshold")

        checkedWithinThreshold
      }

  def setLastCheckedAt(at: SDate): Unit = lastCheckedAt = Some(at)
}

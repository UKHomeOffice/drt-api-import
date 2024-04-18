package advancepassengerinfo.health

import drtlib.SDate

import scala.concurrent.duration.{DurationLong, FiniteDuration}

case class LastCheckedState(now: () => SDate) {
  private var lastCheckedAt: Option[SDate] = None

  def hasCheckedSince(threshold: FiniteDuration): Boolean =
    lastCheckedAt
      .exists { lca =>
        val lastCheckedAgo = (now().millisSinceEpoch - lca.millisSinceEpoch).millis
        lastCheckedAgo <= threshold
      }

  def setLastCheckedAt(at: SDate): Unit = lastCheckedAt = Some(at)
}

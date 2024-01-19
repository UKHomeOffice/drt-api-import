package advancepassengerinfo.health

import java.time.{Duration, Instant}

case class ProcessState() {
  var healthState: Instant = Instant.now()

  def hasCheckedSince: Boolean = {
    val fiveMinutes = 5
    Duration.between(healthState, Instant.now()).compareTo(Duration.ofMinutes(fiveMinutes)) < 0
  }

  def setLastCheckedAt(): Unit = {
    healthState = Instant.now()
  }
}

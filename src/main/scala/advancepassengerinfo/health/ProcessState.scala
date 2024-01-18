package advancepassengerinfo.health

import java.time.{Duration, Instant}

case class ProcessState() {
   var healthState: Instant = Instant.now()

  def isLatest: Boolean = {
    val anHour = Duration.ofHours(1)
    Duration.between(healthState, Instant.now()).compareTo(anHour) < 0
  }

  def update(): Unit = {
    healthState = Instant.now()
  }
}

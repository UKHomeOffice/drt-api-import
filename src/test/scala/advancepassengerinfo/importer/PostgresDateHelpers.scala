package advancepassengerinfo.importer

import drtlib.SDate
import org.joda.time.DateTimeConstants

object PostgresDateHelpers {
  object DayOfTheWeek {
    val Sunday = 0
    val Monday = 1
    val Tuesday = 2
    val Wednesday = 3
    val Thursday = 4
    val Friday = 5
    val Saturday = 6
  }

  def dayOfTheWeek(date: SDate): Int = date.dayOfWeek match {
    case DateTimeConstants.SUNDAY => DayOfTheWeek.Sunday
    case DateTimeConstants.MONDAY => DayOfTheWeek.Monday
    case DateTimeConstants.TUESDAY => DayOfTheWeek.Tuesday
    case DateTimeConstants.WEDNESDAY => DayOfTheWeek.Wednesday
    case DateTimeConstants.THURSDAY => DayOfTheWeek.Thursday
    case DateTimeConstants.FRIDAY => DayOfTheWeek.Friday
    case DateTimeConstants.SATURDAY => DayOfTheWeek.Saturday
  }
}

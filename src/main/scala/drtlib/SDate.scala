package drtlib

import drtlib.SDate.implicits
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.{Logger, LoggerFactory}

case class SDate(dateTime: DateTime) {

  import implicits._

  def getDayOfWeek(): Int = dateTime.getDayOfWeek

  def getFullYear(): Int = dateTime.getYear

  def getMonth(): Int = dateTime.getMonthOfYear

  def getDate(): Int = dateTime.getDayOfMonth

  def addDays(daysToAdd: Int): SDate = dateTime.plusDays(daysToAdd)

  def millisSinceEpoch: Long = dateTime.getMillis

}


object SDate {
  val log: Logger = LoggerFactory.getLogger(getClass)

  object implicits {

    implicit def jodaToSDate(dateTime: DateTime): SDate = SDate(dateTime)

  }

  val yyyyMMdd : SDate => String = date => f"${date.getFullYear() - 2000}${date.getMonth()}%02d${date.getDate()}%02d"

  def jodaSDateToIsoString(dateTime: SDate): String = {
    val fmt = ISODateTimeFormat.dateTimeNoMillis()
    val dt = dateTime.dateTime
    fmt.print(dt)
  }

  def apply(dateTime: String): SDate = SDate(new DateTime(dateTime, DateTimeZone.UTC))

  def now(): SDate = SDate(new DateTime(DateTimeZone.UTC))
}

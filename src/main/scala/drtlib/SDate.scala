package drtlib

import SDate.{implicits, jodaSDateToIsoString}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

case class SDate(dateTime: DateTime) {

  import implicits._

  def getDayOfWeek(): Int = dateTime.getDayOfWeek

  def getFullYear(): Int = dateTime.getYear

  def getMonth(): Int = dateTime.getMonthOfYear

  def getDate(): Int = dateTime.getDayOfMonth

  def getHours(): Int = dateTime.getHourOfDay

  def getMinutes(): Int = dateTime.getMinuteOfHour

  def getSeconds(): Int = dateTime.getSecondOfMinute

  def addDays(daysToAdd: Int): SDate = dateTime.plusDays(daysToAdd)

  def addMonths(monthsToAdd: Int): SDate = dateTime.plusMonths(monthsToAdd)

  def addHours(hoursToAdd: Int): SDate = dateTime.plusHours(hoursToAdd)

  def addMinutes(mins: Int): SDate = dateTime.plusMinutes(mins)

  def addMillis(millisToAdd: Int): SDate = dateTime.plusMillis(millisToAdd)

  def millisSinceEpoch: Long = dateTime.getMillis

  def toISOString(): String = jodaSDateToIsoString(dateTime)

  def getZone(): String = dateTime.getZone.getID

  def getTimeZoneOffsetMillis(): Long = dateTime.getZone.getOffset(millisSinceEpoch)

}


object SDate {
  val log: Logger = LoggerFactory.getLogger(getClass)

  object implicits {

    implicit def jodaToSDate(dateTime: DateTime): SDate = SDate(dateTime)

  }

  def jodaSDateToIsoString(dateTime: SDate): String = {
    val fmt = ISODateTimeFormat.dateTimeNoMillis()
    val dt = dateTime.asInstanceOf[SDate].dateTime
    fmt.print(dt)
  }

  def apply(dateTime: String): SDate = SDate(new DateTime(dateTime, DateTimeZone.UTC))

  def apply(dateTime: String, timeZone: DateTimeZone): SDate = SDate(new DateTime(dateTime, timeZone))

  def apply(dateTime: SDate, timeZone: DateTimeZone): SDate = SDate(new DateTime(dateTime.millisSinceEpoch, timeZone))

  def apply(millis: Long): SDate = SDate(new DateTime(millis, DateTimeZone.UTC))

  def apply(millis: Long, timeZone: DateTimeZone): SDate = SDate(new DateTime(millis, timeZone))

  def now(): SDate = SDate(new DateTime(DateTimeZone.UTC))

  def now(dtz: DateTimeZone): SDate = SDate(new DateTime(dtz))

  def apply(y: Int, m: Int, d: Int, h: Int, mm: Int): SDate = implicits.jodaToSDate(new DateTime(y, m, d, h, mm, DateTimeZone.UTC))

  def apply(y: Int, m: Int, d: Int, h: Int, mm: Int, dateTimeZone: DateTimeZone): SDate = implicits.jodaToSDate(new DateTime(y, m, d, h, mm, dateTimeZone))

  def tryParseString(dateTime: String) = Try(apply(dateTime))
}

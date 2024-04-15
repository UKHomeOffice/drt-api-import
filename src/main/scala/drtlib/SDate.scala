package drtlib

import drtlib.SDate.implicits
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.{Logger, LoggerFactory}

case class SDate(dateTime: DateTime) {

  import implicits._

  def dayOfWeek: Int = dateTime.getDayOfWeek

  def fullYear: Int = dateTime.getYear

  def month: Int = dateTime.getMonthOfYear

  def date: Int = dateTime.getDayOfMonth

  def hour: Int = dateTime.getHourOfDay

  def minute: Int = dateTime.getMinuteOfHour

  def addDays(daysToAdd: Int): SDate = dateTime.plusDays(daysToAdd)

  def millisSinceEpoch: Long = dateTime.getMillis

}


object SDate {
  val log: Logger = LoggerFactory.getLogger(getClass)

  object implicits {

    implicit def jodaToSDate(dateTime: DateTime): SDate = SDate(dateTime)

  }

  val yyyyMMdd: SDate => String = date => f"${date.fullYear - 2000}${date.month}%02d${date.date}%02d"

  def apply(dateTime: String): SDate = SDate(new DateTime(dateTime, DateTimeZone.UTC))

  def now(): SDate = SDate(new DateTime(DateTimeZone.UTC))
}

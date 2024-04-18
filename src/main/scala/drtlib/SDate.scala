package drtlib

import drtlib.SDate.implicits
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.FiniteDuration

case class SDate(dateTime: DateTime) {

  import implicits._

  def dayOfWeek: Int = dateTime.getDayOfWeek

  def fullYear: Int = dateTime.getYear

  def month: Int = dateTime.getMonthOfYear

  def date: Int = dateTime.getDayOfMonth

  def minute: Int = dateTime.getMinuteOfHour

  def plus(i: FiniteDuration): SDate = dateTime.plus(i.toMillis)

  def minus(i: FiniteDuration): SDate = dateTime.minus(i.toMillis)

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

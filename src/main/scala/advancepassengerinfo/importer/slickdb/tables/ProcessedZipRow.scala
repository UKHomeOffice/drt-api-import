package advancepassengerinfo.importer.slickdb.tables

import java.sql.Timestamp
import scala.util.matching.Regex

case class ProcessedZipRow(zip_file_name: String, success: Boolean, processed_at: Timestamp, created_on: Option[String])

object ProcessedZipRow {
  private val dqFileNameDateRegex: Regex = "drt_dq_([0-9]{2})([0-9]{2})([0-9]{2})_[0-9]{6}_[0-9]{4}\\.zip".r

  def extractCreatedOn(fileName: String): Option[String] = fileName match {
    case dqFileNameDateRegex(year, month, day) => Option(s"20$year-$month-$day")
    case _ => None
  }
}

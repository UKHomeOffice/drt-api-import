package advancepassengerinfo.importer.slickdb.tables

import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import slick.lifted.{Rep, Tag}

import java.sql.{Date, Timestamp}

class ProcessedZipTable(tag: Tag) extends Table[ProcessedZipRow](tag, Option("public"), "processed_zip") {
  val zip_file_name: Rep[String] = column[String]("zip_file_name")
  val success: Rep[Boolean] = column[Boolean]("success")
  val processed_at: Rep[Timestamp] = column[Timestamp]("processed_at")
  val created_on: Rep[Option[String]] = column[Option[String]]("created_on")

  def * = (zip_file_name, success, processed_at, created_on).mapTo[ProcessedZipRow]
}

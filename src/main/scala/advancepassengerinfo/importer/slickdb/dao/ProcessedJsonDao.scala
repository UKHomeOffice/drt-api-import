package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonRow, ProcessedJsonTable}
import advancepassengerinfo.manifests.VoyageManifest

import java.sql.Timestamp
import scala.concurrent.Future
import scala.util.Try


trait ProcessedJsonDao {
  def persistJsonFile(zipFileName: String,
                      jsonFileName: String,
                      successful: Boolean,
                      dateIsSuspicious: Boolean,
                      maybeManifest: Option[VoyageManifest],
                      processedAt: Long,
                     ): Future[Unit]
//  def insert(row: ProcessedJsonRow): Future[Unit]

  def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean]
}

case class ProcessedJsonDaoImpl(db: Db) extends ProcessedJsonDao {
  private val table = TableQuery[ProcessedJsonTable]

  def persistJsonFile(zipFileName: String,
                      jsonFileName: String,
                      successful: Boolean,
                      dateIsSuspicious: Boolean,
                      maybeManifest: Option[VoyageManifest],
                      processedAt: Long,
                     ): Future[Unit] = {
    val processedAtTs = new Timestamp(processedAt)
    insert(ProcessedJsonRow(
      zip_file_name = zipFileName,
      json_file_name = jsonFileName,
      suspicious_date = dateIsSuspicious,
      success = successful,
      processed_at = processedAtTs,
      arrival_port_code = maybeManifest.map(_.ArrivalPortCode),
      departure_port_code = maybeManifest.map(_.DeparturePortCode),
      voyage_number = Try(maybeManifest.map(_.VoyageNumber.toInt)).toOption.flatten,
      scheduled = maybeManifest.flatMap(m => m.scheduleArrivalDateTime.map(s => new Timestamp(s.millisSinceEpoch))),
      event_code = maybeManifest.map(_.EventCode),
      non_interactive_total_count = maybeManifest.map(_.PassengerList.count(!_.PassengerIdentifier.exists(_ != ""))),
      non_interactive_trans_count = maybeManifest.map(_.PassengerList.count(p => !p.PassengerIdentifier.exists(_ != "") && p.InTransitFlag.contains("Y"))),
      interactive_total_count = maybeManifest.map(_.PassengerList.count(_.PassengerIdentifier.exists(_ != ""))),
      interactive_trans_count = maybeManifest.map(_.PassengerList.count(p => p.PassengerIdentifier.exists(_ != "") && p.InTransitFlag.contains("Y"))),
    ))
  }

  private def insert(row: ProcessedJsonRow): Future[Unit] =
    db.run(DBIO.seq(table += row))

  def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] = {
    val query = table.filter(r => r.zip_file_name === zipFileName && r.json_file_name === jsonFileName)
    db.run(query.exists.result)
  }
}

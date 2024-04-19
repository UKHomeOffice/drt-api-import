package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonRow, ProcessedJsonTable, ProcessedZipTable}
import advancepassengerinfo.manifests.VoyageManifest

import java.sql.Timestamp
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


trait ProcessedJsonDao {
  def persistJsonFile(zipFileName: String,
                      jsonFileName: String,
                      successful: Boolean,
                      dateIsSuspicious: Boolean,
                      maybeManifest: Option[VoyageManifest],
                      processedAt: Long,
                     ): Future[Unit]

  def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean]
}

case class ProcessedJsonDaoImpl(db: Db)
                               (implicit ec: ExecutionContext) extends ProcessedJsonDao {
  private val table = TableQuery[ProcessedJsonTable]
  private val zipTable = TableQuery[ProcessedZipTable]

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
      carrier_code = maybeManifest.map(_.CarrierCode),
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

  def earliestUnpopulatedDate: Future[Option[String]] = {
    val query = table join zipTable on {
      case (json, zip) => json.zip_file_name === zip.zip_file_name
    } filter {
      case (json, _) => json.voyage_number.isEmpty
    } sortBy {
      case (_, zip) => zip.created_on.asc
    } map {
      case (_, zip) => zip.created_on
    } take 1

    db.run(query.result).map(_.headOption.flatten)
  }

  def updateManifestColumnsForDate(date: String): Future[Int] = {
    val query = sql"""UPDATE processed_json pj
          set (arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled, event_code,
            non_interactive_total_count, non_interactive_trans_count, interactive_total_count, interactive_trans_count) = (
              select
                arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled_date, event_code,
                count(*) filter (where passenger_identifier = '') as non_interactive_total_count,
                count(*) filter (where passenger_identifier = '' and in_transit=true) as non_interactive_trans_count,
                count(*) filter (where passenger_identifier != '') as interactive_total_count,
                count(*) filter (where passenger_identifier != '' and in_transit=true) as interactive_trans_count
              from voyage_manifest_passenger_info vm
              where vm.json_file = pj.json_file_name
              group by arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled_date, event_code
            )
          from processed_zip pz
          where pj.zip_file_name = pz.zip_file_name and pj.scheduled is null and pz.created_on == '$date';
       """

    db.run(query.asUpdate)
  }

  def delete(jsonFileName: String): Future[Int] = {
    val query = table.filter(_.json_file_name === jsonFileName).delete
    db.run(query)
  }
}

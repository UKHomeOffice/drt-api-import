package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonRow, ProcessedJsonTable, ProcessedZipTable}
import drtlib.SDate

import scala.concurrent.{ExecutionContext, Future}


trait ProcessedJsonDao {
  def insert(row: ProcessedJsonRow): Future[Unit]

  def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean]

  def earliestUnpopulatedDate(since: Long): Future[Option[String]]

  def populateManifestColumnsForDate(date: String): Future[Int]

  def delete(jsonFileName: String): Future[Int]
}

case class ProcessedJsonDaoImpl(db: Db)
                               (implicit ec: ExecutionContext) extends ProcessedJsonDao {
  private val table = TableQuery[ProcessedJsonTable]
  private val zipTable = TableQuery[ProcessedZipTable]

  def insert(row: ProcessedJsonRow): Future[Unit] = db.run(DBIO.seq(table += row))

  def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] = {
    val query = table.filter(r => r.zip_file_name === zipFileName && r.json_file_name === jsonFileName)
    db.run(query.exists.result)
  }

  def earliestUnpopulatedDate(since: Long): Future[Option[String]] = {
    val query = table join zipTable on {
      case (json, zip) => json.zip_file_name === zip.zip_file_name
    } filter {
      case (json, zip) =>
        json.voyage_number.isEmpty && json.success && zip.success && zip.created_on >= SDate(since).toIsoDate
    } sortBy {
      case (_, zip) => zip.created_on.asc
    } map {
      case (_, zip) => zip.created_on
    } take 1

    db.run(query.result).map(_.headOption.flatten)
  }

  def populateManifestColumnsForDate(date: String): Future[Int] = {
    val query = sql"""UPDATE processed_json pj
          set (arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled, event_code,
            non_interactive_total_count, non_interactive_trans_count, interactive_total_count, interactive_trans_count) = (
              select
                vm.arrival_port_code, vm.departure_port_code, COALESCE(vm.voyage_number, -1) as voyage_number, vm.carrier_code, vm.scheduled_date, vm.event_code,
                count(*) filter (where passenger_identifier = '') as non_interactive_total_count,
                count(*) filter (where passenger_identifier = '' and in_transit=true) as non_interactive_trans_count,
                count(*) filter (where passenger_identifier != '') as interactive_total_count,
                count(*) filter (where passenger_identifier != '' and in_transit=true) as interactive_trans_count
              from processed_json pj_
              left join voyage_manifest_passenger_info vm on pj_.json_file_name = vm.json_file
              where pj_.json_file_name=pj.json_file_name and pj_.zip_file_name=pz.zip_file_name
              group by vm.arrival_port_code, vm.departure_port_code, vm.voyage_number, vm.carrier_code, vm.scheduled_date, vm.event_code
              limit 1
            )
          from processed_zip pz
          where pj.zip_file_name = pz.zip_file_name and pz.created_on = $date;
       """

    db.run(query.asUpdate)
  }

  def delete(jsonFileName: String): Future[Int] = {
    val query = table.filter(_.json_file_name === jsonFileName).delete
    db.run(query)
  }
}

/**
 select
 vm.arrival_port_code, vm.departure_port_code, COALESCE(vm.voyage_number, -1) as voyage_number, vm.carrier_code, vm.scheduled_date, vm.event_code,
 count(*) filter (where passenger_identifier = '') as non_interactive_total_count,
                count(*) filter (where passenger_identifier = '' and in_transit=true) as non_interactive_trans_count,
 count(*) filter (where passenger_identifier != '') as interactive_total_count,
                count(*) filter (where passenger_identifier != '' and in_transit=true) as interactive_trans_count
 from processed_json pj
 left join voyage_manifest_passenger_info vm on pj.json_file_name = vm.json_file
 where pj.json_file_name = 'drt_190429_055500_SK0803_CI_7615.json'
 group by vm.arrival_port_code, vm.departure_port_code, vm.voyage_number, vm.carrier_code, vm.scheduled_date, vm.event_code
 limit 1;

 ProcessedJsonRow("test1.zip", "test.json", false, true, 1970-01-01 01:00:00.0, Some("LHR"), Some("JFK"), Some(1000), Some("BA"), Some(2021-01-01 01:30:00.0), Some("DC"), Some(0), Some(0), Some(2), Some(0))
 ProcessedJsonRow("test1.zip", "test.json", false, true, 1970-01-01 01:00:00.0, Some("LHR"), Some("JFK"), Some(1000), Some("BA"), Some(2021-01-01 01:30:00.0), Some("DC"), Some(0), Some(0), Some(1), Some(0))
 */

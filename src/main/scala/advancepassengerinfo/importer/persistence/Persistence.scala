package advancepassengerinfo.importer.persistence

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonRow, ProcessedJsonTable, ProcessedZipRow, ProcessedZipTable}
import advancepassengerinfo.importer.slickdb.{VoyageManifestPassengerInfoDao, tables}
import advancepassengerinfo.manifests.VoyageManifest
import com.typesafe.scalalogging.Logger
import drtlib.SDate
import slick.lifted.TableQuery

import java.sql.Timestamp
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

trait Persistence {
  def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]]

  def persistJsonFile(zipFileName: String,
                      jsonFileName: String,
                      successful: Boolean,
                      dateIsSuspicious: Boolean,
                      maybeManifest: Option[VoyageManifest],
                      processedAt: Long,
                     ): Future[Int]

  def persistZipFile(zipFileName: String, successful: Boolean, processedAt: Long): Future[Boolean]

  def lastPersistedFileName: Future[Option[String]]

  def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean]
}

trait DbPersistence extends Persistence {
  private val log = Logger(getClass)

  val db: Db

  implicit val ec: ExecutionContext

  private val manifestTable = VoyageManifestPassengerInfoDao
  private val processedJsonTable = TableQuery[ProcessedJsonTable]
  private val processedZipTable = TableQuery[ProcessedZipTable]

  val con: db.profile.backend.DatabaseDef = db.con

  override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] = {
    manifest.scheduleArrivalDateTime
      .map { scheduledDate =>
        dayOfWeekAndWeekOfYear(scheduledDate)
          .flatMap {
            case (dayOfWeek, weekOfYear) =>
              val (rowCount, action) = manifestTable.rowsToInsert(manifest, dayOfWeek, weekOfYear, jsonFileName)
              con.run(action).map(_ => Option(rowCount))
          }
          .recover {
            case t =>
              log.error(s"Failed to persist manifest", t)
              None
          }
      }
      .getOrElse {
        log.error(s"Failed to get a scheduled time for ${manifest.DeparturePortCode} > ${manifest.ArrivalPortCode} :: ${manifest.CarrierCode}-${manifest.VoyageNumber} :: ${manifest.ScheduledDateOfArrival}T${manifest.ScheduledDateOfArrival}")
        Future.successful(None)
      }
  }

  private def dayOfWeekAndWeekOfYear(date: SDate): Future[(Int, Int)] = {
    val schTs = new Timestamp(date.millisSinceEpoch)

    con.run(manifestTable.dayOfWeekAndWeekOfYear(schTs)).collect {
      case Some((dow, woy)) => (dow, woy)
    }
  }

  override def persistJsonFile(zipFileName: String,
                               jsonFileName: String,
                               successful: Boolean,
                               dateIsSuspicious: Boolean,
                               maybeManifest: Option[VoyageManifest],
                               processedAt: Long,
                              ): Future[Int] = {
    val processedAtTs = new Timestamp(processedAt)
    val processedJsonFileToInsert = processedJsonTable += ProcessedJsonRow(
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
    )
    con.run(processedJsonFileToInsert)
  }

  override def persistZipFile(zipFileName: String, successful: Boolean, processedAt: Long): Future[Boolean] = {
    val processedAtTs = new Timestamp(processedAt)
    val maybeCreatedOn = ProcessedZipRow.extractCreatedOn(zipFileName)
    val processedZipFileToInsert = processedZipTable += tables.ProcessedZipRow(zipFileName, successful, processedAtTs, maybeCreatedOn)
    con.run(processedZipFileToInsert).map(_ > 0)
  }

  override def lastPersistedFileName: Future[Option[String]] = {
    val sourceFileNamesQuery = processedJsonTable.map(_.zip_file_name)
    con.run(sourceFileNamesQuery.max.result)
  }

  override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] = {
    val query = processedJsonTable.filter(r => r.zip_file_name === zipFileName && r.json_file_name === jsonFileName)
    con.run(query.exists.result)
  }
}

case class DbPersistenceImpl(db: Db)
                            (implicit executionContext: ExecutionContext) extends DbPersistence {
  override implicit val ec: ExecutionContext = executionContext
}

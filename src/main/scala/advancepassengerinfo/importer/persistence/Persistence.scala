package advancepassengerinfo.importer.persistence

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.VoyageManifestPassengerInfoTable
import advancepassengerinfo.manifests.VoyageManifest
import com.typesafe.scalalogging.Logger
import drtlib.SDate

import java.sql.Timestamp
import scala.concurrent.{ExecutionContext, Future}

trait Persistence {
  def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]]

  def persistJsonFile(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean): Future[Int]

  def persistZipFile(zipFileName: String, successful: Boolean): Future[Boolean]

  def lastPersistedFileName: Future[Option[String]]
}

case class PersistenceImp(db: Db)
                         (implicit ec: ExecutionContext) extends Persistence {
  private val log = Logger(getClass)

  val manifestTable: VoyageManifestPassengerInfoTable = VoyageManifestPassengerInfoTable(db.tables)

  val con: db.tables.profile.backend.DatabaseDef = db.con

  import db.tables.profile.api._

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

  override def persistJsonFile(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean): Future[Int] = {
    val processedAt = new Timestamp(SDate.now().millisSinceEpoch)
    val processedJsonFileToInsert = db.tables.ProcessedJson += db.tables.ProcessedJsonRow(zipFileName, jsonFileName, dateIsSuspicious, successful, processedAt)
    con.run(processedJsonFileToInsert)
  }

  override def persistZipFile(zipFileName: String, successful: Boolean): Future[Boolean] = {
    val processedAt = new Timestamp(SDate.now().millisSinceEpoch)
    val processedZipFileToInsert = db.tables.ProcessedZip += db.tables.ProcessedZipRow(zipFileName, successful, processedAt)
    con.run(processedZipFileToInsert).map(_ > 0)
  }

  override def lastPersistedFileName: Future[Option[String]] = {
    val sourceFileNamesQuery = db.tables.ProcessedJson.map(_.zip_file_name)
    con.run(sourceFileNamesQuery.max.result)
  }
}

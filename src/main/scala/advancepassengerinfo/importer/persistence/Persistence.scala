package advancepassengerinfo.importer.persistence

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.VoyageManifestPassengerInfoTable
import advancepassengerinfo.manifests.VoyageManifest
import drtlib.SDate

import java.sql.Timestamp
import scala.concurrent.{ExecutionContext, Future}

trait Persistence {
  def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]]

  def persistJsonFile(zipFileName: String, jsonFileName: String, wasSuccessful: Boolean, dateIsSuspicious: Boolean): Future[Int]

  def persistZipFile(zipFileName: String, success: Boolean): Future[Boolean]
}

case class PersistenceImp(db: Db)
                         (implicit ec: ExecutionContext) extends Persistence {
  val manifestTable: VoyageManifestPassengerInfoTable = VoyageManifestPassengerInfoTable(db.tables)

  val con: db.tables.profile.backend.DatabaseDef = db.con

  import db.tables.profile.api._

  override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] = {
    manifest.scheduleArrivalDateTime.map { scheduledDate =>
      dayOfWeekAndWeekOfYear(scheduledDate).flatMap {
        case (dayOfWeek, weekOfYear) =>
          val (rowCount, action) = manifestTable.rowsToInsert(manifest, dayOfWeek, weekOfYear, jsonFileName)
          con.run(action).map(_ => Option(rowCount))
      }
    }.getOrElse(Future.successful(None))
  }

  private def dayOfWeekAndWeekOfYear(date: SDate): Future[(Int, Int)] = {
    val schTs = new Timestamp(date.millisSinceEpoch)

    con.run(manifestTable.dayOfWeekAndWeekOfYear(schTs)).collect {
      case Some((dow, woy)) => (dow, woy)
    }
  }

  override def persistJsonFile(zipFileName: String, jsonFileName: String, wasSuccessful: Boolean, dateIsSuspicious: Boolean): Future[Int] = {
    val processedAt = new Timestamp(SDate.now().millisSinceEpoch)
    val processedJsonFileToInsert = db.tables.ProcessedJson += db.tables.ProcessedJsonRow(zipFileName, jsonFileName, dateIsSuspicious, wasSuccessful, processedAt)
    con.run(processedJsonFileToInsert)
  }

  override def persistZipFile(zipFileName: String, success: Boolean): Future[Boolean] = {
    val processedAt = new Timestamp(SDate.now().millisSinceEpoch)
    val processedZipFileToInsert = db.tables.ProcessedZip += db.tables.ProcessedZipRow(zipFileName, success, processedAt)
    con.run(processedZipFileToInsert).map(_ > 0)
  }

}

package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.serialisation.VoyageManifestSerialisation.voyageManifestRows
import advancepassengerinfo.importer.slickdb.tables.VoyageManifestPassengerInfoTable
import advancepassengerinfo.manifests.VoyageManifest
import drtlib.SDate

import java.sql.Timestamp
import scala.concurrent.{ExecutionContext, Future}


trait VoyageManifestPassengerInfoDao {
  def persistManifest(jsonFileName: String, manifest: VoyageManifest, scheduledDate: SDate): Future[Option[Int]]
}

case class VoyageManifestPassengerInfoDaoImpl(db: Db)
                                             (implicit ec: ExecutionContext) extends VoyageManifestPassengerInfoDao {
  private val voyageManifestPassengerInfo = TableQuery[VoyageManifestPassengerInfoTable]

  def persistManifest(jsonFileName: String, manifest: VoyageManifest, scheduledDate: SDate): Future[Option[Int]] =
    dayOfWeekAndWeekOfYear(new Timestamp(scheduledDate.millisSinceEpoch))
      .flatMap {
        case (dayOfWeek, weekOfYear) =>
          insertManifest(manifest, dayOfWeek, weekOfYear, jsonFileName)
            .map(Option(_))
      }

  def insertManifest(vm: VoyageManifest, dayOfWeek: Int, weekOfYear: Int, jsonFile: String): Future[Int] = {
    val rows = voyageManifestRows(vm, dayOfWeek, weekOfYear, jsonFile)
    db.run(DBIO.seq(voyageManifestPassengerInfo ++= rows)).map(_ => rows.size)
  }

  def dayOfWeekAndWeekOfYear(date: Timestamp)
                            (implicit ec: ExecutionContext): Future[(Int, Int)] =
    db.run(sql"""SELECT EXTRACT(DOW FROM TIMESTAMP'#$date'), EXTRACT(WEEK FROM TIMESTAMP'#$date')""".as[(Int, Int)].map {
      _.headOption match {
        case Some((dayOfWeek, weekOfYear)) => (dayOfWeek, weekOfYear)
        case None =>
          throw new Exception("Failed to get day of week and week of year from date")
      }
    })
}

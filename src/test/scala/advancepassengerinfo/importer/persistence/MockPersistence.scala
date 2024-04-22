package advancepassengerinfo.importer.persistence

import advancepassengerinfo.importer.persistence.MockPersistence.{JsonFileCall, ManifestCall, ZipFileCall}
import advancepassengerinfo.importer.slickdb.dao.{ProcessedJsonDao, ProcessedZipDao, VoyageManifestPassengerInfoDao}
import advancepassengerinfo.importer.slickdb.tables.{ProcessedZipRow, VoyageManifestPassengerInfoRow}
import advancepassengerinfo.manifests.VoyageManifest
import akka.actor.ActorRef
import drtlib.SDate

import scala.concurrent.Future

object MockPersistence {
  case class ManifestCall(rows: String)

  case class JsonFileCall(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean)

  case class ZipFileCall(zipFileName: String, successful: Boolean)
}

case class MockZipDao(probe: ActorRef) extends ProcessedZipDao {
  override def insert(row: ProcessedZipRow): Future[Unit] = {
    probe ! ZipFileCall(row.zip_file_name, row.success)
    Future.successful()
  }

  override def lastPersistedFileName: Future[Option[String]] = Future.successful(Option("_"))

  override def delete(zipFileName: String): Future[Int] = Future.successful(1)

  override def oldestDate: Future[Option[String]] = Future.successful(Option("2021-01-01"))
}

case class MockJsonDao(probe: ActorRef) extends ProcessedJsonDao {
  override def insert(row: advancepassengerinfo.importer.slickdb.tables.ProcessedJsonRow): Future[Unit] = {
    probe ! JsonFileCall(row.zip_file_name, row.json_file_name, row.success, row.suspicious_date)
    Future.successful()
  }
  override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] = Future.successful(false)

  override def earliestUnpopulatedDate: Future[Option[String]] = Future.successful(Option("2021-01-01"))

  override def updateManifestColumnsForDate(date: String): Future[Int] = Future.successful(1)

  override def delete(jsonFileName: String): Future[Int] = Future.successful(1)
}

case class MockVoyageManifestPassengerInfoDao(probe: ActorRef) extends VoyageManifestPassengerInfoDao {
  override def insert(rows: Seq[VoyageManifestPassengerInfoRow]): Future[Int] = {
    probe ! ManifestCall(rows.headOption.map(_.json_file).getOrElse(""))
    Future.successful(rows.size)
  }

  override def dayOfWeekAndWeekOfYear(date: java.sql.Timestamp): Future[(Int, Int)] = Future.successful((1, 1))
  override def delete(jsonFileName: String): Future[Int] = Future.successful(1)
}

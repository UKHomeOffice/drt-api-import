package advancepassengerinfo.importer.persistence

import advancepassengerinfo.importer.persistence.MockPersistence.{JsonFileCall, ManifestCall, ZipFileCall}
import advancepassengerinfo.importer.slickdb.dao.{ProcessedJsonDao, ProcessedZipDao, VoyageManifestPassengerInfoDao}
import advancepassengerinfo.importer.slickdb.tables.ProcessedZipRow
import advancepassengerinfo.manifests.VoyageManifest
import akka.actor.ActorRef
import drtlib.SDate

import scala.concurrent.Future

object MockPersistence {
  case class ManifestCall(jsonFileName: String, manifest: VoyageManifest)

  case class JsonFileCall(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean)

  case class ZipFileCall(zipFileName: String, successful: Boolean)
}

//case class MockPersistence(probe: ActorRef) extends Persistence {
//  override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] = {
//    probe ! ManifestCall(jsonFileName, manifest)
//    Future.successful(Option(1))
//  }
//
//  override def persistJsonFile(zipFileName: String,
//                               jsonFileName: String,
//                               successful: Boolean,
//                               dateIsSuspicious: Boolean,
//                               maybeManifest: Option[VoyageManifest],
//                               processedAt: Long,
//                              ): Future[Int] = {
//    probe ! JsonFileCall(zipFileName, jsonFileName, successful, dateIsSuspicious)
//    Future.successful(1)
//  }
//
//  override def persistZipFile(zipFileName: String, successful: Boolean, processedAt: Long): Future[Unit] = {
//    probe ! ZipFileCall(zipFileName, successful)
//    Future.successful()
//  }
//
//  override def lastPersistedFileName: Future[Option[String]] = Future.successful(Option("_"))
//
//  override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] = Future.successful(false)
//}

case class MockZipDao(probe: ActorRef) extends ProcessedZipDao {
  override def insert(row: ProcessedZipRow): Future[Unit] = {
    probe ! ZipFileCall(row.zip_file_name, row.success)
    Future.successful()
  }

  override def lastPersistedFileName: Future[Option[String]] = Future.successful(Option("_"))
}

case class MockJsonDao(probe: ActorRef) extends ProcessedJsonDao {
  override def persistJsonFile(zipFileName: String,
                               jsonFileName: String,
                               successful: Boolean,
                               dateIsSuspicious: Boolean,
                               maybeManifest: Option[VoyageManifest],
                               processedAt: Long,
                              ): Future[Unit] = {
    probe ! JsonFileCall(zipFileName, jsonFileName, successful, dateIsSuspicious)
    Future.successful()
  }

  override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] = Future.successful(false)
}

case class MockVoyageManifestPassengerInfoDao(probe: ActorRef) extends VoyageManifestPassengerInfoDao {
  override def persistManifest(jsonFileName: String, manifest: VoyageManifest, scheduledDate: SDate): Future[Option[Int]] = {
    probe ! ManifestCall(jsonFileName, manifest)
    Future.successful(Option(1))
  }
}

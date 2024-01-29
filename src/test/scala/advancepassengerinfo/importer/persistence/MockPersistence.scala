package advancepassengerinfo.importer.persistence

import advancepassengerinfo.importer.persistence.MockPersistence.{JsonFileCall, ManifestCall, ZipFileCall}
import advancepassengerinfo.manifests.VoyageManifest
import akka.actor.ActorRef

import scala.concurrent.Future

object MockPersistence {
  case class ManifestCall(jsonFileName: String, manifest: VoyageManifest)

  case class JsonFileCall(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean)

  case class ZipFileCall(zipFileName: String, successful: Boolean)
}

case class MockPersistence(probe: ActorRef) extends Persistence {
  override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] = {
    probe ! ManifestCall(jsonFileName, manifest)
    Future.successful(Option(1))
  }

  override def persistJsonFile(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean): Future[Int] = {
    probe ! JsonFileCall(zipFileName, jsonFileName, successful, dateIsSuspicious)
    Future.successful(1)
  }

  override def persistZipFile(zipFileName: String, successful: Boolean): Future[Boolean] = {
    probe ! ZipFileCall(zipFileName, successful)
    Future.successful(true)
  }

  override def lastPersistedFileName: Future[Option[String]] = Future.successful(Option("_"))

  override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] = Future.successful(false)
}

case class MockPersistenceWithRecovery(probe: ActorRef) extends Persistence {
  private var isFirstCall: Boolean = true

  override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] =
    if (isFirstCall) {
      isFirstCall = false
      Future.failed(new Exception("db persistManifest exception"))
    } else {
      probe ! ManifestCall(jsonFileName, manifest)
      Future.successful(Option(1))
    }

  override def persistJsonFile(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean): Future[Int] =
    if (isFirstCall) {
      isFirstCall = false
      Future.failed(new Exception("db persistJsonFile exception"))
    } else {
      probe ! JsonFileCall(zipFileName, jsonFileName, successful, dateIsSuspicious)
      Future.successful(1)
    }

  override def persistZipFile(zipFileName: String, successful: Boolean): Future[Boolean] =
    if (isFirstCall) {
      isFirstCall = false
      Future.failed(new Exception("db persistZipFile exception"))
    } else {
      probe ! ZipFileCall(zipFileName, successful)
      Future.successful(true)
    }

  override def lastPersistedFileName: Future[Option[String]] =
    Future.successful(Option("1.zip"))

  override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] =
    if (isFirstCall) {
      isFirstCall = false
      Future.failed(new Exception(s"db jsonHasBeenProcessed exception $isFirstCall"))
    } else
      Future.successful(false)
}

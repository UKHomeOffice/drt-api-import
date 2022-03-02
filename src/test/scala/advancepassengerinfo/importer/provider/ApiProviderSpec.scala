package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.persistence.{ManifestPersistor, Persistence}
import advancepassengerinfo.importer.slickdb.{Builder, VoyageManifestPassengerInfoTable}
import advancepassengerinfo.importer.{InMemoryDatabase, PostgresDateHelpers}
import advancepassengerinfo.manifests.{PassengerInfo, VoyageManifest}
import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.testkit.TestProbe
import drtlib.SDate
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.sql.Timestamp
import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.language.postfixOps
import scala.util.{Success, Try}

case class MockDqFileProcessor(probe: ActorRef) extends DqFileProcessor {
  override val process: String => Source[Option[Int], NotUsed] = objectKey => {
    probe ! objectKey
    Source(List(Option(1)))
  }
}

case class MockPersistence(probe: ActorRef) extends Persistence {
  override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] = {
    probe ! jsonFileName
    Future.successful(Option(1))
  }

  override def persistJsonFile(zipFileName: String, jsonFileName: String, wasSuccessful: Boolean, dateIsSuspicious: Boolean): Future[Int] = {
    probe ! jsonFileName
    Future.successful(1)
  }

  override def persistZipFile(zipFileName: String, success: Boolean): Future[Boolean] = {
    probe ! zipFileName
    Future.successful(true)
  }
}

case class MockS3FileNamesProvider(files: List[List[String]]) extends S3FileNamesProvider {
  private var filesQueue = files
  override val nextFiles: String => Future[List[String]] = (previous: String) => filesQueue match {
    case Nil => Future.successful(List(previous))
    case head :: tail =>
      filesQueue = tail
      val files = if (previous.nonEmpty) previous :: head else head
      Future.successful(files)
  }
}

class ApiProviderSpec extends AnyWordSpec with Matchers with Builder {
  implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
  implicit val materializer: Materializer = Materializer.createMaterializer(actorSystem)
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val vmTable: VoyageManifestPassengerInfoTable = VoyageManifestPassengerInfoTable(InMemoryDatabase.tables)
  val provider: TestApiProvider = TestApiProvider()
  val persistor: ManifestPersistor = ManifestPersistor(InMemoryDatabase, 6)

  "An importer" should {
    "send all the files from an s3 file name provider in sequence" in {
      val filesProbe = TestProbe("files")
      val persistenceProbe = TestProbe("persistence")

      val batchedFileNames = List(List("a", "b"), List("c", "d"), List("e", "f"))
      val mockS3Files = DqFileNameProvider(MockS3FileNamesProvider(batchedFileNames))
      val mockProcessor = MockDqFileProcessor(filesProbe.ref)
      val mockPersistence = MockPersistence(persistenceProbe.ref)
      val importer = DqFileImporterImpl(mockS3Files, mockProcessor, mockPersistence, 100.millis)

      val killSwitch: UniqueKillSwitch = importer.processFilesAfter("").viaMat(KillSwitches.single)(Keep.right).toMat(Sink.ignore)(Keep.left).run()

      batchedFileNames.flatten.foreach(f => filesProbe.expectMsg(f))

      killSwitch.shutdown()
    }
  }

    "A provider with a valid zip containing one json manifest" should {
      "return a success case representation of that processed zip" in {
        val manifestsStream: Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed] = provider.manifestsStream("")

        val stuff: immutable.Seq[(String, Try[List[(String, Try[VoyageManifest])]])] = Await.result(manifestsStream.runWith(Sink.seq), 1 second)

        val expected = Vector(
          ("manifest.zip", Success(List(("manifest.json", Success(VoyageManifest("DC", "STN", "BRE", "3631", "FR", "2016-03-02", "07:30:00", List(
            PassengerInfo(Some("P"), "MAR", "", Some("21"), Some("STN"), "N", Some("GBR"), Some("MAR"), Some("000")),
            PassengerInfo(Some("G"), "", "", Some("43"), Some("STN"), "N", Some("GBR"), Some(""), Some(""))))))))))

        stuff should be(expected)
      }
    }

    "A provider with a single valid manifest with both an iAPI and a non-iAPI passenger" should {
      "result in only the iAPI passenger record being recorded when passed through a persistor" in {
        import advancepassengerinfo.importer.InMemoryDatabase.tables.profile.api._

        val manifestsStream: Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed] = provider.manifestsStream("")

        Await.ready(persistor.addPersistenceToStream(manifestsStream).runWith(Sink.seq), 1 second)

        val paxEntries = InMemoryDatabase.tables.VoyageManifestPassengerInfo.result
        val paxRows = Await.result(InMemoryDatabase.con.run(paxEntries), 1 second)

        val schDate = SDate("2016-03-02T07:30:00.0")
        val schTs = new Timestamp(schDate.millisSinceEpoch)
        val dayOfWeek = PostgresDateHelpers.dayOfTheWeek(schDate)
        val expected = Vector(InMemoryDatabase.tables.VoyageManifestPassengerInfoRow("DC", "STN", "BRE", 3631, "FR", schTs, dayOfWeek, 9, "P", "MAR", "", 21, "STN", "N", "GBR", "MAR", "000", in_transit = false, "manifest.json"))

        paxRows should be(expected)
      }
    }
}

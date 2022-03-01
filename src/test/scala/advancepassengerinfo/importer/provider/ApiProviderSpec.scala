package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.persistence.ManifestPersistor
import advancepassengerinfo.importer.slickdb.{Builder, VoyageManifestPassengerInfoTable}
import advancepassengerinfo.importer.{InMemoryDatabase, PostgresDateHelpers}
import advancepassengerinfo.manifests.{PassengerInfo, VoyageManifest}
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.testkit.TestProbe
import akka.{Done, NotUsed}
import drtlib.SDate
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import java.sql.Timestamp
import scala.collection.immutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps
import scala.language.postfixOps
import scala.util.{Success, Try}

trait DqFileImporter {
  def processFilesAfter(lastFileName: String): Source[String, NotUsed]
}

case class DqFileImporterImpl(fileNameProvider: DqFileNameProvider,
                              fileProcessor: DqFileProcessor,
                              throttle: FiniteDuration)
                             (implicit ec: ExecutionContext) extends DqFileImporter {
  def processFilesAfter(lastFileName: String): Source[String, NotUsed] =
    Source
      .unfoldAsync((lastFileName, List[String]())) { case (lastFileName, lastFiles) =>
        fileNameProvider.markerAndNextFileNames(lastFileName).map {
          case (nextFetch, newFiles) => Option((nextFetch, newFiles), (lastFileName, lastFiles))
        }
      }
      .throttle(1, throttle)
      .map { x =>
        println(s"x: $x")
        x
      }
      .map(_._2)
      .mapConcat(identity)
      .mapAsync(1) { f =>
        fileProcessor.process(f).map(_ => f)
      }
}

trait DqFileProcessor {
  val process: String => Future[Done]
}

case class MockDqFileProcessor(probe: ActorRef) extends DqFileProcessor {
  override val process: String => Future[Done] = (objectKey: String) => {
    probe ! objectKey
    Future.successful(Done)
  }
}

trait S3FileNamesProvider {
  val nextFiles: String => Future[List[String]]
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

case class S3FileNamesProviderImpl(s3Client: S3AsyncClient, bucket: String)
                                  (implicit ec: ExecutionContext) extends S3FileNamesProvider {
  override val nextFiles: String => Future[List[String]] = (lastFile: String) => s3Client
    .listObjects(ListObjectsRequest.builder().bucket(bucket).maxKeys(5).marker(lastFile).build()).asScala
    .map(_.contents().asScala.map(_.key()).toList)
}

case class DqFileNameProvider(s3FileNamesProvider: S3FileNamesProvider)
                             (implicit ec: ExecutionContext) {
  val markerAndNextFileNames: String => Future[(String, List[String])] =
    (lastFile: String) => s3FileNamesProvider.nextFiles(lastFile)
      .map { fileNames =>
        val files = if (lastFile.nonEmpty) fileNames.filterNot(_.contains(lastFile)) else fileNames
        val nextFetch = files.sorted.reverse.headOption.getOrElse(lastFile)
        (nextFetch, files)
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
      val probe = TestProbe("api")

      val batchedFileNames = List(List("a", "b"), List("c", "d"), List("e", "f"))
      val mockS3Files = DqFileNameProvider(MockS3FileNamesProvider(batchedFileNames))
      val importer = DqFileImporterImpl(mockS3Files, MockDqFileProcessor(probe.ref), 100.millis)

      val killSwitch: UniqueKillSwitch = importer.processFilesAfter("").viaMat(KillSwitches.single)(Keep.right).toMat(Sink.ignore)(Keep.left).run()

      batchedFileNames.flatten.foreach(f => probe.expectMsg(f))

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

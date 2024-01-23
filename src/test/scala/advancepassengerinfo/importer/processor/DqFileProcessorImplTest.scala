package advancepassengerinfo.importer.processor

import advancepassengerinfo.generator.ManifestGenerator
import advancepassengerinfo.importer.persistence.MockPersistence.{JsonFileCall, ManifestCall, ZipFileCall}
import advancepassengerinfo.importer.persistence.{DbPersistence, MockPersistence, Persistence}
import advancepassengerinfo.importer.provider.{FileNames, Manifests, MockFileNames, MockStatsDCollector}
import advancepassengerinfo.importer.{Db, DqApiFeedImpl, InMemoryDatabase}
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{TestKit, TestProbe}
import drtlib.SDate
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import drtlib.SDate.yyyyMMdd
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}


case class MockManifests(manifestTries: List[List[Try[Seq[(String, Try[VoyageManifest])]]]]) extends Manifests {
  var zipManifestTries: List[List[Try[Seq[(String, Try[VoyageManifest])]]]] = manifestTries
  override def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed] = zipManifestTries match {
    case Nil => Source.empty
    case head :: tail =>
      zipManifestTries = tail
      Source(head)
  }
}

case class PersistenceWithProbe(db: Db, probe: ActorRef)(implicit val ec: ExecutionContext) extends DbPersistence {
  override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] = {
    probe ! ManifestCall(jsonFileName, manifest)
    super.persistManifest(jsonFileName, manifest)
  }

  override def persistJsonFile(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean): Future[Int] = {
    probe ! JsonFileCall(zipFileName, jsonFileName, successful, dateIsSuspicious)
    super.persistJsonFile(zipFileName, jsonFileName, successful, dateIsSuspicious)
  }

  override def persistZipFile(zipFileName: String, successful: Boolean): Future[Boolean] = {
    probe ! ZipFileCall(zipFileName, successful)
    super.persistZipFile(zipFileName, successful)
  }
}

class DqFileProcessorTest extends TestKit(ActorSystem("MySpec"))
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    InMemoryDatabase.truncateDb()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val zipFileName = "some.zip"
  val jsonFileName = "manifest.json"
  val mockZipFailureProvider: MockManifests = MockManifests(List(List(Failure(new Exception("bad zip")))))
  val jsonWithFailedManifest: (String, Failure[Nothing]) = (jsonFileName, Failure(new Exception("bad json")))

  val manifest: VoyageManifest = ManifestGenerator.manifest()
  val jsonWithSuccessfulManifest: (String, Success[VoyageManifest]) = (jsonFileName, Success(manifest))

  def singleZipMockProvider(jsonWithManifests: Seq[(String, Try[VoyageManifest])]): MockManifests = MockManifests(List(List(Success(jsonWithManifests))))

  "A DQ file processor" should {

    "Persist a failed zip file" in {
      val probe = TestProbe("probe")
      val mockPersistence = MockPersistence(probe.ref)
      val processor = DqFileProcessorImpl(mockZipFailureProvider, mockPersistence)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ZipFileCall(zipFileName, successful = false))

      result should ===(Seq(None))
    }

    "Persist a failed json file, and a failed zip file" in {
      val probe = TestProbe("probe")
      val mockPersistence = MockPersistence(probe.ref)
      val processor = DqFileProcessorImpl(singleZipMockProvider(Seq(jsonWithFailedManifest)), mockPersistence)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(JsonFileCall(zipFileName, jsonFileName, successful = false, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = false))

      result should ===(Seq(Option(1, 0)))
    }

    "Persist a manifest, successful json file and successful zip file" in {
      val probe = TestProbe("probe")
      val mockPersistence = MockPersistence(probe.ref)
      val processor = DqFileProcessorImpl(singleZipMockProvider(Seq(jsonWithSuccessfulManifest)), mockPersistence)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall(jsonFileName, manifest))
      probe.expectMsg(JsonFileCall(zipFileName, jsonFileName, successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result should ===(Seq(Option(1, 1)))
    }

    "Persist multiple successful manifests" in {
      val probe = TestProbe("probe")
      val mockPersistence = MockPersistence(probe.ref)
      val manifests = Seq(
        ("1.json", Success(manifest)),
        ("2.json", Success(manifest)),
        ("3.json", Success(manifest)),
      )
      val processor = DqFileProcessorImpl(singleZipMockProvider(manifests), mockPersistence)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("1.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("2.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "2.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("3.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "3.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result should ===(Seq(Option(3, 3)))
    }

    "Persist multiple manifests with some failures" in {
      val probe = TestProbe("probe")
      val mockPersistence = MockPersistence(probe.ref)
      val manifests = Seq(
        ("1.json", Success(manifest)),
        ("2.json", Failure(new Exception("failed"))),
        ("3.json", Success(manifest)),
      )
      val processor = DqFileProcessorImpl(singleZipMockProvider(manifests), mockPersistence)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("1.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(JsonFileCall(zipFileName, "2.json", successful = false, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("3.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "3.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result should ===(Seq(Option(3, 2)))
    }

    "Persist a zip-json combination only once" in {
      val probe = TestProbe("probe")
      val mockPersistence = PersistenceWithProbe(InMemoryDatabase, probe.ref)
      val manifests1 = Seq(
        ("1.json", Success(manifest)),
        ("2.json", Success(manifest)),
      )
      val processor1 = DqFileProcessorImpl(singleZipMockProvider(manifests1), mockPersistence)
      val result1 = Await.result(processor1.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("1.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("2.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "2.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result1 should ===(Seq(Option(2, 2)))

      val manifests2 = Seq(
        ("2.json", Success(manifest)),
        ("3.json", Success(manifest)),
      )
      val processor2 = DqFileProcessorImpl(singleZipMockProvider(manifests2), mockPersistence)
      val result2 = Await.result(processor2.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("3.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "3.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result2 should ===(Seq(Option(1, 1)))
    }
  }

  "DqApiFeedImpl" should {
    "Continue to process files after zip failure and manifest failure" in {
      val failureThenSuccess = MockManifests(List(
        List(Failure(new Exception("bad zip"))),
        List(Success(Seq(jsonWithFailedManifest))),
        List(Success(Seq(jsonWithSuccessfulManifest))),
      ))
      val probe = TestProbe("persistence")
      val processor = DqFileProcessorImpl(failureThenSuccess, MockPersistence(probe.ref))

      val dqApiFeed = DqApiFeedImpl(
        MockFileNames(List(List("a", "b"), List("c"))),
        processor,
        100.millis,
        MockStatsDCollector
      )

      dqApiFeed.processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ZipFileCall("a", successful = false))
      probe.expectMsg(JsonFileCall("b", jsonFileName, successful = false, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("b", successful = false))
      probe.expectMsg(ManifestCall(jsonFileName, manifest))
      probe.expectMsg(JsonFileCall("c", jsonFileName, successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("c", successful = true))

    }

    "handle exceptions while getting nextFiles and start from fallbackFile" in {
      val fallbackFileName = "drt_dq_" + yyyyMMdd(SDate.now().addDays(-1)) + "_000000_0000.zip"
      val mockFileNamesProvider = new FileNames {
        val fallBackListFiles: String => Future[List[String]] = fallbackFileName => Future.sequence(List(
          Future.successful(List(fallbackFileName, "3.zip")),
        )).map(_.flatten)

        val s3Files: String => Future[List[String]] = previous => Future.sequence(List(
          Future.successful(List("1.zip", "2.zip")),
          Future.failed(new Exception("next file exception")),
        )).map(_.flatten)

        override val nextFiles: (String, String) => Future[List[String]] = (lastFile: String, fallbackFileName) => s3Files(lastFile)
          .recoverWith {
            case _ =>
              fallBackListFiles(fallbackFileName)
          }.map(_.filterNot(_ == lastFile))
      }
      val probe = TestProbe("probe")
      val mockPersistence = MockPersistence(probe.ref)

      val manifests = Seq(
        (s"manifest1.json", Success(manifest)),
        (s"manifest2.json", Success(manifest)),
      )
      val processor = DqFileProcessorImpl(singleZipMockProvider(manifests), mockPersistence)
      val dqApiFeed: DqApiFeedImpl = DqApiFeedImpl(
        mockFileNamesProvider,
        processor,
        100.millis,
        MockStatsDCollector
      )

      dqApiFeed.processFilesAfter("1.zip").runWith(Sink.seq)
      probe.expectMsg(ManifestCall("manifest1.json", manifest))
      probe.expectMsg(JsonFileCall(fallbackFileName, s"manifest1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("manifest2.json", manifest))
      probe.expectMsg(JsonFileCall(fallbackFileName, "manifest2.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(fallbackFileName, successful = true))

    }

    "handle exception while persisting" in {
      val mockPersistence = new Persistence {
        override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] = Future.failed(new Exception("db exception"))

        override def persistJsonFile(zipFileName: String, jsonFileName: String, successful: Boolean, dateIsSuspicious: Boolean): Future[Int] = Future.failed(new Exception("db exception"))

        override def persistZipFile(zipFileName: String, successful: Boolean): Future[Boolean] = Future.failed(new Exception("db exception"))

        override def lastPersistedFileName: Future[Option[String]] = Future.failed(new Exception("db exception"))

        override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] = Future.failed(new Exception("db exception"))
      }

      val manifests = Seq(
        ("1.json", Success(manifest)),
        ("2.json", Success(manifest)),
        ("3.json", Success(manifest)),
        ("4.json", Success(manifest)),
      )
      val processor = DqFileProcessorImpl(singleZipMockProvider(manifests), mockPersistence)
      val dqApiFeed: DqApiFeedImpl = DqApiFeedImpl(
        MockFileNames(List(List("1.zip", "2.zip"), List("3.zip", "4.zip"))),
        processor,
        100.millis,
        MockStatsDCollector
      )

      val result = Await.result(dqApiFeed.processFilesAfter("_").runWith(Sink.seq), 1.second)

      result should ===(Vector.empty)
    }
  }
}

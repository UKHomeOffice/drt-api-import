package advancepassengerinfo.importer.processor

import advancepassengerinfo.generator.ManifestGenerator
import advancepassengerinfo.health.LastCheckedState
import advancepassengerinfo.importer.persistence.MockPersistence.{JsonFileCall, ManifestCall, ZipFileCall}
import advancepassengerinfo.importer.persistence.{DbPersistence, MockPersistence}
import advancepassengerinfo.importer.provider.{FileNames, Manifests, MockFileNames, MockStatsDCollector}
import advancepassengerinfo.importer.{Db, DqApiFeedImpl, InMemoryDatabase}
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}


case class MockManifestsWithZipFileName(manifestTries: Map[String, List[List[Try[Seq[(String, Try[VoyageManifest])]]]]]) extends Manifests {
  var zipManifestTries: Map[String, List[List[Try[Seq[(String, Try[VoyageManifest])]]]]] = manifestTries

  override def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed] = {
    zipManifestTries.get(fileName) match {
      case Some(manifests) if manifests.nonEmpty =>
        val reducedManifests = manifests.reduce((a, b) => a ++ b)
        zipManifestTries = zipManifestTries.updated(fileName, List(reducedManifests))
        Source(reducedManifests)
      case _ => Source.empty
    }
  }
}

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

  override def persistJsonFile(zipFileName: String,
                               jsonFileName: String,
                               successful: Boolean,
                               dateIsSuspicious: Boolean,
                               maybeManifest: Option[VoyageManifest],
                               processedAt: Long,
                              ): Future[Int] = {
    probe ! JsonFileCall(zipFileName, jsonFileName, successful, dateIsSuspicious)
    super.persistJsonFile(zipFileName, jsonFileName, successful, dateIsSuspicious, maybeManifest, processedAt)
  }

  override def persistZipFile(zipFileName: String, successful: Boolean, processedAt: Long): Future[Boolean] = {
    probe ! ZipFileCall(zipFileName, successful)
    super.persistZipFile(zipFileName, successful, 0L)
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

  def multipleZipMockProvider(JsonWithManifestsForFiles: Map[String, List[List[Try[Seq[(String, Try[VoyageManifest])]]]]]): MockManifestsWithZipFileName =
    MockManifestsWithZipFileName(JsonWithManifestsForFiles)

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
        MockStatsDCollector,
        LastCheckedState()
      )

      dqApiFeed.processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ZipFileCall("a", successful = false))
      probe.expectMsg(JsonFileCall("b", jsonFileName, successful = false, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("b", successful = false))
      probe.expectMsg(ManifestCall(jsonFileName, manifest))
      probe.expectMsg(JsonFileCall("c", jsonFileName, successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("c", successful = true))

    }

    "handle exceptions while getting nextFiles and recovery to continue" in {
      def createManifests(manifest: VoyageManifest): Seq[(String, Try[VoyageManifest])] = Seq(
        ("manifest1.json", Success(manifest)),
        ("manifest2.json", Success(manifest)),
      )

      def createMockFileNamesProvider: FileNames = new FileNames {
        private var isFirstCall: Boolean = true
        private val s3Files: String => Future[List[String]] = _ => Future.sequence(List(
          Future.successful(List("1.zip", "2.zip")),
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception("next file exception"))
          } else Future.successful(List("3.zip")),
        )).map(_.flatten)

        override val nextFiles: String => Future[List[String]] = (lastFile: String) => s3Files(lastFile)
      }

      val mockFileNamesProvider = createMockFileNamesProvider
      val probe = TestProbe("probe")
      val mockPersistence = MockPersistence(probe.ref)
      val manifests = createManifests(manifest)
      val processor = DqFileProcessorImpl(singleZipMockProvider(manifests), mockPersistence)
      val dqApiFeed: DqApiFeedImpl = DqApiFeedImpl(mockFileNamesProvider, processor, 100.millis, MockStatsDCollector, LastCheckedState())

      dqApiFeed.processFilesAfter("1.zip").runWith(Sink.seq)

      probe.expectMsg(ManifestCall("manifest1.json", manifest))
      probe.expectMsg(JsonFileCall("2.zip", "manifest1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("manifest2.json", manifest))
      probe.expectMsg(JsonFileCall("2.zip", "manifest2.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("2.zip", successful = true))
    }

    val batchedFileNames = List(List("1.zip", "2.zip"), List("3.zip", "4.zip"))

    val manifestsWithFileName: Map[String, List[List[Try[Seq[(String, Try[VoyageManifest])]]]]] = Map(
      "1.zip" -> List(List(Success(Seq(("manifest1.json", Success(manifest)))))),
      "2.zip" -> List(List(Success(Seq(("manifest2.json", Success(manifest)))))),
      "3.zip" -> List(List(Success(Seq(("manifest3.json", Success(manifest)))))),
      "4.zip" -> List(List(Success(Seq(("manifest4.json", Success(manifest))))))
    )

    def getDqApiFeedInstance(mockPersistence: MockPersistence): DqApiFeedImpl = {
      val processor = DqFileProcessorImpl(multipleZipMockProvider(manifestsWithFileName), mockPersistence)
      DqApiFeedImpl(
        MockFileNames(batchedFileNames),
        processor,
        200.millis,
        MockStatsDCollector,
        LastCheckedState()
      )
    }

    def afterRecoveryExpectedMsg(probe: TestProbe): ZipFileCall = {
      probe.expectMsg(ManifestCall("manifest2.json", manifest))
      probe.expectMsg(JsonFileCall("2.zip", "manifest2.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("2.zip", successful = true))
      probe.expectMsg(ManifestCall("manifest3.json", manifest))
      probe.expectMsg(JsonFileCall("3.zip", "manifest3.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("3.zip", successful = true))
      probe.expectMsg(ManifestCall("manifest4.json", manifest))
      probe.expectMsg(JsonFileCall("4.zip", "manifest4.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("4.zip", successful = true))
    }

    "process and persist zip files successful with manifest json" in {
      val probe: TestProbe = TestProbe("probe")
      val mockPersistence =  MockPersistence(probe.ref)
      val dqApiFeed = getDqApiFeedInstance(mockPersistence)

      dqApiFeed.processFilesAfter("_").runWith(Sink.seq)
      probe.expectMsg(ManifestCall("manifest1.json", manifest))
      probe.expectMsg(JsonFileCall("1.zip", "manifest1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("1.zip", successful = true))
      afterRecoveryExpectedMsg(probe)
    }

    "process zip files and recover while exception in jsonHasBeenProcessed" in {
      val probe: TestProbe = TestProbe("probe")
      var isFirstCall: Boolean = true
      val mockPersistence = new MockPersistence(probe.ref) {
        override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] =
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception(s"db jsonHasBeenProcessed exception $isFirstCall"))
          } else {
            Future.successful(false)
          }
      }

      getDqApiFeedInstance(mockPersistence).processFilesAfter("_").runWith(Sink.seq)
      probe.expectMsg(ZipFileCall("1.zip", successful = false))
      afterRecoveryExpectedMsg(probe)

    }

    "process zip files and recover while exception in persistManifest" in {
      val probe: TestProbe = TestProbe("probe")
      var isFirstCall: Boolean = true
      val mockPersistence = new MockPersistence(probe.ref) {
        override def persistManifest(jsonFileName: String, manifest: VoyageManifest): Future[Option[Int]] =
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception("db persistManifest exception"))
          } else {
            probe ! ManifestCall(jsonFileName, manifest)
            Future.successful(Option(1))
          }
      }

      val dqApiFeed = getDqApiFeedInstance(mockPersistence)
      dqApiFeed.processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ZipFileCall("1.zip", successful = false))
      afterRecoveryExpectedMsg(probe)

    }

    "process zip files and recover while exception in persistZipFile" in {
      val probe: TestProbe = TestProbe("probe")
      var isFirstCall: Boolean = true
      val mockPersistence = new MockPersistence(probe.ref) {
        override def persistZipFile(zipFileName: String, successful: Boolean, processedAt: Long): Future[Boolean] =
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception("db persistZipFile exception"))
          } else {
            probe ! ZipFileCall(zipFileName, successful)
            Future.successful(true)
          }

      }

      getDqApiFeedInstance(mockPersistence).processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ManifestCall("manifest1.json", manifest))
      probe.expectMsg(JsonFileCall("1.zip", "manifest1.json", successful = true, dateIsSuspicious = false))
      afterRecoveryExpectedMsg(probe)

    }

    "process zip files and recover while exception in persistJsonFile" in {
      val probe: TestProbe = TestProbe("probe")
      var isFirstCall: Boolean = true
      val mockPersistence = new MockPersistence(probe.ref) {
        override def persistJsonFile(zipFileName: String,
                                     jsonFileName: String,
                                     successful: Boolean,
                                     dateIsSuspicious: Boolean,
                                     maybeManifest: Option[VoyageManifest],
                                     processedAt: Long,
                                    ): Future[Int] =
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception("db persistJsonFile exception"))
          } else {
            probe ! JsonFileCall(zipFileName, jsonFileName, successful, dateIsSuspicious)
            Future.successful(1)
          }
      }

      getDqApiFeedInstance(mockPersistence).processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ManifestCall("manifest1.json", manifest))
      probe.expectMsg(ZipFileCall("1.zip", successful = true))
      afterRecoveryExpectedMsg(probe)

    }

  }
}

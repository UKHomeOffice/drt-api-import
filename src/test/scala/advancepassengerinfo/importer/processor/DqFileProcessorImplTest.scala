package advancepassengerinfo.importer.processor

import advancepassengerinfo.generator.ManifestGenerator
import advancepassengerinfo.health.LastCheckedState
import advancepassengerinfo.importer.persistence.MockPersistence.{JsonFileCall, ManifestCall, ZipFileCall}
import advancepassengerinfo.importer.persistence.{MockJsonDao, MockVoyageManifestPassengerInfoDao, MockZipDao}
import advancepassengerinfo.importer.provider.{FileNames, Manifests, MockFileNames, MockStatsDCollector}
import advancepassengerinfo.importer.slickdb.dao.{ProcessedJsonDao, ProcessedJsonDaoImpl, ProcessedZipDao, ProcessedZipDaoImpl}
import advancepassengerinfo.importer.slickdb.tables.ProcessedZipRow
import advancepassengerinfo.importer.{DqApiFeedImpl, InMemoryDatabase}
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{TestKit, TestProbe}
import drtlib.SDate
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
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


class DqFileProcessorTest extends TestKit(ActorSystem("MySpec"))
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    InMemoryDatabase.dropAndCreateTables
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val zipFileName = "some.zip"
  val jsonFileName = "manifest.json"
  val validManifest: VoyageManifest = ManifestGenerator.manifest("2022-06-01", "10:00")
  val badScheduledDateManifest: VoyageManifest = ManifestGenerator.manifest("2022x06x01", "10x00")

  val mockZipFailureProvider: MockManifests = MockManifests(List(List(Failure(new Exception("bad zip")))))

  val jsonWithFailedManifest: (String, Failure[Nothing]) = (jsonFileName, Failure(new Exception("bad json")))
  val jsonWithBadScheduledDateManifest: (String, Success[VoyageManifest]) = (jsonFileName, Success(badScheduledDateManifest))
  val jsonWithSuccessfulManifest: (String, Success[VoyageManifest]) = (jsonFileName, Success(validManifest))

  def singleZipMockProvider(jsonWithManifests: Seq[(String, Try[VoyageManifest])]): MockManifests = MockManifests(List(List(Success(jsonWithManifests))))

  def multipleZipMockProvider(JsonWithManifestsForFiles: Map[String, List[List[Try[Seq[(String, Try[VoyageManifest])]]]]]): MockManifestsWithZipFileName =
    MockManifestsWithZipFileName(JsonWithManifestsForFiles)

  private val probe = TestProbe("probe")
  private val mockZipDao = MockZipDao(probe.ref)
  private val mockJsonDao = MockJsonDao(probe.ref)
  private val mockManifestsDao = MockVoyageManifestPassengerInfoDao(probe.ref)

  "A DQ file processor" should {
    "Persist a failed zip file" in {
      val processor = DqFileProcessorImpl(mockZipFailureProvider, mockZipDao, mockJsonDao, mockManifestsDao)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ZipFileCall(zipFileName, successful = false))

      result should ===(Seq(None))
    }

    "Persist a failed json file, and a failed zip file" in {
      val processor = DqFileProcessorImpl(singleZipMockProvider(Seq(jsonWithFailedManifest)), mockZipDao, mockJsonDao, mockManifestsDao)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(JsonFileCall(zipFileName, jsonFileName, successful = false, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = false))

      result should ===(Seq(Option(1, 0)))
    }

    "Persist a manifest, successful json file and successful zip file" in {
      val processor = DqFileProcessorImpl(singleZipMockProvider(Seq(jsonWithSuccessfulManifest)), mockZipDao, mockJsonDao, mockManifestsDao)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall(jsonFileName, validManifest))
      probe.expectMsg(JsonFileCall(zipFileName, jsonFileName, successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result should ===(Seq(Option(1, 1)))
    }

    "Fail a manifest with an invalid scheduled date - unsuccessful json file and unsuccessful zip file" in {
      val processor = DqFileProcessorImpl(singleZipMockProvider(Seq(jsonWithBadScheduledDateManifest)), mockZipDao, mockJsonDao, mockManifestsDao)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(JsonFileCall(zipFileName, jsonFileName, successful = false, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = false))

      result should ===(Seq(Option(1, 0)))
    }

    "Persist multiple successful manifests" in {
      val manifests = Seq(
        ("1.json", Success(validManifest)),
        ("2.json", Success(validManifest)),
        ("3.json", Success(validManifest)),
      )
      val processor = DqFileProcessorImpl(singleZipMockProvider(manifests), mockZipDao, mockJsonDao, mockManifestsDao)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("1.json", validManifest))
      probe.expectMsg(JsonFileCall(zipFileName, "1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("2.json", validManifest))
      probe.expectMsg(JsonFileCall(zipFileName, "2.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("3.json", validManifest))
      probe.expectMsg(JsonFileCall(zipFileName, "3.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result should ===(Seq(Option(3, 3)))
    }

    "Persist multiple manifests with some failures" in {
      val manifests = Seq(
        ("1.json", Success(validManifest)),
        ("2.json", Failure(new Exception("failed"))),
        ("3.json", Success(validManifest)),
      )
      val processor = DqFileProcessorImpl(singleZipMockProvider(manifests), mockZipDao, mockJsonDao, mockManifestsDao)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("1.json", validManifest))
      probe.expectMsg(JsonFileCall(zipFileName, "1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(JsonFileCall(zipFileName, "2.json", successful = false, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("3.json", validManifest))
      probe.expectMsg(JsonFileCall(zipFileName, "3.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result should ===(Seq(Option(3, 2)))
    }

    "Persist a zip-json combination only once" in {
      val manifests1 = Seq(
        ("1.json", Success(validManifest)),
        ("2.json", Success(validManifest)),
      )
      val zipDao = ProcessedZipDaoImpl(InMemoryDatabase)
      val jsonDao = ProcessedJsonDaoImpl(InMemoryDatabase)
      val processor1 = DqFileProcessorImpl(singleZipMockProvider(manifests1), zipDao, jsonDao, mockManifestsDao)
      val result1 = Await.result(processor1.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("1.json", validManifest))
      probe.expectMsg(ManifestCall("2.json", validManifest))

      result1 should ===(Seq(Option(2, 2)))

      val manifests2 = Seq(
        ("2.json", Success(validManifest)),
        ("3.json", Success(validManifest)),
      )
      val processor2 = DqFileProcessorImpl(singleZipMockProvider(manifests2), zipDao, jsonDao, mockManifestsDao)
      val result2 = Await.result(processor2.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("3.json", validManifest))

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
      val processor = DqFileProcessorImpl(failureThenSuccess, mockZipDao, mockJsonDao, mockManifestsDao)

      val dqApiFeed = DqApiFeedImpl(
        MockFileNames(List(List("a", "b"), List("c"))),
        processor,
        100.millis,
        MockStatsDCollector,
        LastCheckedState(() => SDate.now())
      )

      dqApiFeed.processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ZipFileCall("a", successful = false))
      probe.expectMsg(JsonFileCall("b", jsonFileName, successful = false, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("b", successful = false))
      probe.expectMsg(ManifestCall(jsonFileName, validManifest))
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
      val manifests = createManifests(validManifest)
      val processor = DqFileProcessorImpl(singleZipMockProvider(manifests), mockZipDao, mockJsonDao, mockManifestsDao)
      val dqApiFeed: DqApiFeedImpl = DqApiFeedImpl(mockFileNamesProvider, processor, 100.millis, MockStatsDCollector, LastCheckedState(() => SDate.now()))

      dqApiFeed.processFilesAfter("1.zip").runWith(Sink.seq)

      probe.expectMsg(ManifestCall("manifest1.json", validManifest))
      probe.expectMsg(JsonFileCall("2.zip", "manifest1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("manifest2.json", validManifest))
      probe.expectMsg(JsonFileCall("2.zip", "manifest2.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("2.zip", successful = true))
    }

    val batchedFileNames = List(List("1.zip", "2.zip"), List("3.zip", "4.zip"))

    val manifestsWithFileName: Map[String, List[List[Try[Seq[(String, Try[VoyageManifest])]]]]] = Map(
      "1.zip" -> List(List(Success(Seq(("manifest1.json", Success(validManifest)))))),
      "2.zip" -> List(List(Success(Seq(("manifest2.json", Success(validManifest)))))),
      "3.zip" -> List(List(Success(Seq(("manifest3.json", Success(validManifest)))))),
      "4.zip" -> List(List(Success(Seq(("manifest4.json", Success(validManifest))))))
    )

    def getDqApiFeedInstance(mockZipDao: ProcessedZipDao,
                             mockJsonDao: ProcessedJsonDao,
                             mockManifestsDao: MockVoyageManifestPassengerInfoDao,
                            ): DqApiFeedImpl = {
      val processor = DqFileProcessorImpl(multipleZipMockProvider(manifestsWithFileName), mockZipDao, mockJsonDao, mockManifestsDao)
      DqApiFeedImpl(
        MockFileNames(batchedFileNames),
        processor,
        200.millis,
        MockStatsDCollector,
        LastCheckedState(() => SDate.now())
      )
    }

    def afterRecoveryExpectedMsg(probe: TestProbe): ZipFileCall = {
      probe.expectMsg(ManifestCall("manifest2.json", validManifest))
      probe.expectMsg(JsonFileCall("2.zip", "manifest2.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("2.zip", successful = true))
      probe.expectMsg(ManifestCall("manifest3.json", validManifest))
      probe.expectMsg(JsonFileCall("3.zip", "manifest3.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("3.zip", successful = true))
      probe.expectMsg(ManifestCall("manifest4.json", validManifest))
      probe.expectMsg(JsonFileCall("4.zip", "manifest4.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("4.zip", successful = true))
    }

    "process and persist zip files successful with manifest json" in {
      val dqApiFeed = getDqApiFeedInstance(mockZipDao, mockJsonDao, mockManifestsDao)

      dqApiFeed.processFilesAfter("_").runWith(Sink.seq)
      probe.expectMsg(ManifestCall("manifest1.json", validManifest))
      probe.expectMsg(JsonFileCall("1.zip", "manifest1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall("1.zip", successful = true))
      afterRecoveryExpectedMsg(probe)
    }

    "process zip files and recover while exception in jsonHasBeenProcessed" in {
      var isFirstCall: Boolean = true
      val mockJsonWithExceptionOnFirstFile = new MockJsonDao(probe.ref) {
        override def jsonHasBeenProcessed(zipFileName: String, jsonFileName: String): Future[Boolean] =
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception(s"db jsonHasBeenProcessed exception $isFirstCall"))
          } else {
            Future.successful(false)
          }
      }

      getDqApiFeedInstance(mockZipDao, mockJsonWithExceptionOnFirstFile, mockManifestsDao).processFilesAfter("_").runWith(Sink.seq)
      probe.expectMsg(ZipFileCall("1.zip", successful = false))
      afterRecoveryExpectedMsg(probe)

    }

    "process zip files and recover while exception in persistManifest" in {
      var isFirstCall: Boolean = true
      val mockManifestDaoWithExceptionOnFirstCall = new MockVoyageManifestPassengerInfoDao(probe.ref) {
        override def persistManifest(jsonFileName: String, manifest: VoyageManifest, scheduledDate: SDate): Future[Option[Int]] =
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception("db persistManifest exception"))
          } else {
            probe ! ManifestCall(jsonFileName, manifest)
            Future.successful(Option(1))
          }
      }

      val dqApiFeed = getDqApiFeedInstance(mockZipDao, mockJsonDao, mockManifestDaoWithExceptionOnFirstCall)
      dqApiFeed.processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ZipFileCall("1.zip", successful = false))
      afterRecoveryExpectedMsg(probe)
    }

    "process zip files and recover while exception in persistZipFile" in {
      var isFirstCall: Boolean = true
      val mockZipDaoWithExceptionOnFirstCall = new MockZipDao(probe.ref) {
        override def insert(row: ProcessedZipRow): Future[Unit] =
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception("db persistZipFile exception"))
          } else {
            probe ! ZipFileCall(row.zip_file_name, row.success)
            Future.successful()
          }

      }

      getDqApiFeedInstance(mockZipDaoWithExceptionOnFirstCall, mockJsonDao, mockManifestsDao).processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ManifestCall("manifest1.json", validManifest))
      probe.expectMsg(JsonFileCall("1.zip", "manifest1.json", successful = true, dateIsSuspicious = false))
      afterRecoveryExpectedMsg(probe)

    }

    "process zip files and recover while exception in persistJsonFile" in {
      var isFirstCall: Boolean = true
      val mockJsonDaoWithExceptionOnFirstCall = new MockJsonDao(probe.ref) {
        override def persistJsonFile(zipFileName: String,
                                     jsonFileName: String,
                                     successful: Boolean,
                                     dateIsSuspicious: Boolean,
                                     maybeManifest: Option[VoyageManifest],
                                     processedAt: Long,
                                    ): Future[Unit] =
          if (isFirstCall) {
            isFirstCall = false
            Future.failed(new Exception("db persistJsonFile exception"))
          } else {
            probe ! JsonFileCall(zipFileName, jsonFileName, successful, dateIsSuspicious)
            Future.successful()
          }
      }

      getDqApiFeedInstance(mockZipDao, mockJsonDaoWithExceptionOnFirstCall, mockManifestsDao).processFilesAfter("_").runWith(Sink.seq)

      probe.expectMsg(ManifestCall("manifest1.json", validManifest))
      probe.expectMsg(ZipFileCall("1.zip", successful = false))
      afterRecoveryExpectedMsg(probe)
    }
  }
}

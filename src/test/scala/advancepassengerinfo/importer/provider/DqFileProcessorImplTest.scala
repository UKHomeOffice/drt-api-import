package advancepassengerinfo.importer.provider

import advancepassengerinfo.generator.ManifestGenerator
import advancepassengerinfo.importer
import advancepassengerinfo.importer.DqFileProcessorImpl
import advancepassengerinfo.importer.persistence.MockPersistence
import advancepassengerinfo.importer.persistence.MockPersistence.{JsonFileCall, ManifestCall, ZipFileCall}
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.{Failure, Success, Try}


case class MockManifests(manifestTries: List[Try[Seq[(String, Try[VoyageManifest])]]]) extends Manifests {
  override def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed] =
    Source(manifestTries)
}

class DqFileProcessorTest extends TestKit(ActorSystem("MySpec"))
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val zipFileName = "some.zip"
  val jsonFileName = "manifest.json"

  "A DQ file processor" should {
    val mockZipFailureProvider = MockManifests(List(Failure(new Exception("bad zip"))))
    val jsonWithFailedManifest = (jsonFileName, Failure(new Exception("bad json")))

    val manifest = ManifestGenerator.manifest()
    val jsonWithSuccessfulManifest = (jsonFileName, Success(manifest))

    def singleZipMockProvider(jsonWithManifests: Seq[(String, Try[VoyageManifest])]) = MockManifests(List(Success(jsonWithManifests)))

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
      val processor = importer.DqFileProcessorImpl(singleZipMockProvider(Seq(jsonWithFailedManifest)), mockPersistence)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(JsonFileCall(zipFileName, jsonFileName, successful = false, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = false))

      result should ===(Seq(Option(1, 0)))
    }

    "Persist a manifest, successful json file and successful zip file" in {
      val probe = TestProbe("probe")
      val mockPersistence = MockPersistence(probe.ref)
      val processor = importer.DqFileProcessorImpl(singleZipMockProvider(Seq(jsonWithSuccessfulManifest)), mockPersistence)
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
      val processor = importer.DqFileProcessorImpl(singleZipMockProvider(manifests), mockPersistence)
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
      val processor = importer.DqFileProcessorImpl(singleZipMockProvider(manifests), mockPersistence)
      val result = Await.result(processor.process(zipFileName).runWith(Sink.seq), 1.second)

      probe.expectMsg(ManifestCall("1.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "1.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(JsonFileCall(zipFileName, "2.json", successful = false, dateIsSuspicious = false))
      probe.expectMsg(ManifestCall("3.json", manifest))
      probe.expectMsg(JsonFileCall(zipFileName, "3.json", successful = true, dateIsSuspicious = false))
      probe.expectMsg(ZipFileCall(zipFileName, successful = true))

      result should ===(Seq(Option(3, 2)))
    }
  }
}

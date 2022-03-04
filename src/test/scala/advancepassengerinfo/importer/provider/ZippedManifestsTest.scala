package advancepassengerinfo.importer.provider

import advancepassengerinfo.manifests.VoyageManifest
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.testkit.TestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.file.{Files, Paths}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object MockFileAsStreamWithException extends FileAsStream {
  override def asInputStream(objectKey: String): Future[InputStream] = Future.failed(new Exception("failed"))
}

case class MockFileAsStreamWithBadZip(resource: String) extends FileAsStream {
  override def asInputStream(objectKey: String): Future[InputStream] = {
    val bytes = Files.readAllBytes(Paths.get(getClass.getResource(resource).getPath))
    Future.successful(new ByteArrayInputStream(bytes))
  }
}

class ZippedManifestsTest extends TestKit(ActorSystem("MySpec"))
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  "A ZippedManifestsProvider" should {
    "handle an exception from the file provider" in {
      val provider: Manifests = ZippedManifests(MockFileAsStreamWithException)
      val result: Seq[Try[Seq[(String, Try[VoyageManifest])]]] = Await.result(provider.tryManifests("some.zip").runWith(Sink.seq), 1.second)

      result.head.getClass should ===(classOf[Failure[Seq[(String, Try[VoyageManifest])]]])
    }

    "handle corrupt zip data" in {
      val provider: Manifests = ZippedManifests(MockFileAsStreamWithBadZip("/manifest-corrupt.zip"))
      val result: Seq[Try[Seq[(String, Try[VoyageManifest])]]] = Await.result(provider.tryManifests("some.zip").runWith(Sink.seq), 1.second)

      result.head.getClass should ===(classOf[Failure[Seq[(String, Try[VoyageManifest])]]])
    }

    "handle valid zip data" in {
      val provider: Manifests = ZippedManifests(MockFileAsStreamWithBadZip("/manifest.zip"))
      val result: Seq[Try[Seq[(String, Try[VoyageManifest])]]] = Await.result(provider.tryManifests("some.zip").runWith(Sink.seq), 1.second)

      result.head.getClass should ===(classOf[Success[Seq[(String, Try[VoyageManifest])]]])
    }
  }
}

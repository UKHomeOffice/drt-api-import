package advancepassengerinfo.importer.provider

import java.io.FileInputStream
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionContext


case class TestApiProvider()(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) extends ApiProviderLike {
  val log = Logger(getClass)

  def filesAsSource: Source[String, NotUsed] = Source(getListOfFiles("").sorted)

  def inputStream(fileName: String): ZipInputStream = {
    val zipByteStream = new FileInputStream(fileName)
    new ZipInputStream(zipByteStream)
  }

  def getListOfFiles(dir: String): List[String] = List("src/test/resources/manifest.zip")
}

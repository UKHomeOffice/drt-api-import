package advancepassengerinfo.importer.provider

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionContext


case class LocalApiProvider(pathToFiles: String)(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) extends ApiProviderLike {
  val log = Logger(getClass)

  def filesAsSource: Source[String, NotUsed] = Source(getListOfFiles(pathToFiles).sorted)

  def inputStream(fileName: String): ZipInputStream = {
    val zipByteStream = new FileInputStream(fileName)
    new ZipInputStream(zipByteStream)
  }

  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)

    if (d.exists && d.isDirectory)
      d.listFiles.filter(_.isFile).map(_.getAbsolutePath).toList
    else
      List[String]()
  }
}

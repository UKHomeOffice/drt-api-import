package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.parser.JsonManifestParser
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger

import java.io.InputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}


trait ManifestsProvider {
  def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed]
}

case class ZippedManifestsProvider(fileProvider: FileAsStream)
                                  (implicit ec: ExecutionContext) extends ManifestsProvider {
  private val log = Logger(getClass)

  override def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed] =
    Source
      .future(zipInputStream(fileName))
      .map {
        case Some(stream) => Try(extractManifests(stream))
        case None => Failure(new Exception(s"Failed to extract zip"))
      }
      .recover {
        case t => Failure(t)
      }

  private def zipInputStream(objectKey: String): Future[Option[ZipInputStream]] =
    inputStream(objectKey).map(_.map(stream => new ZipInputStream(stream)))

  private def inputStream(objectKey: String): Future[Option[InputStream]] =
    fileProvider
      .asInputStream(objectKey)
      .map(Option(_))

  private def extractManifests(zipInputStream: ZipInputStream): List[(String, Try[VoyageManifest])] =
    LazyList
      .continually(zipInputStream.getNextEntry)
      .takeWhile(_ != null)
      .map { zipEntry =>
        (zipEntry.getName, readStreamToString(zipInputStream))
      }
      .map {
        case (jsonFileName, jsonFileContent) =>
          (jsonFileName, JsonManifestParser.parseVoyagePassengerInfo(jsonFileContent))
      }
      .toList

  private def readStreamToString(zipInputStream: ZipInputStream): String = {
    val buffer = new Array[Byte](4096)
    val stringBuffer = new ArrayBuffer[Byte]()
    var len: Int = zipInputStream.read(buffer)

    while (len > 0) {
      stringBuffer ++= buffer.take(len)
      len = zipInputStream.read(buffer)
    }
    new String(stringBuffer.toArray, UTF_8)
  }
}

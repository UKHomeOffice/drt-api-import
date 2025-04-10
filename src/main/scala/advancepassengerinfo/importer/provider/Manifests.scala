package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.parser.JsonManifestParser
import advancepassengerinfo.manifests.VoyageManifest
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}


trait Manifests {
  def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed]
}

case class ZippedManifests(fileProvider: FileAsStream)
                          (implicit ec: ExecutionContext) extends Manifests {
  override def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed] =
    Source
      .future(zipInputStream(fileName))
      .map(stream => Try(extractManifests(stream)))
      .recover {
        case t => Failure(t)
      }

  private def zipInputStream(objectKey: String): Future[ZipInputStream] =
    fileProvider
      .asInputStream(objectKey)
      .map(new ZipInputStream(_))

  private def extractManifests(zipInputStream: ZipInputStream): List[(String, Try[VoyageManifest])] =
    LazyList
      .continually(zipInputStream.getNextEntry)
      .takeWhile(_ != null)
      .map(zipEntry => (zipEntry.getName, readStreamToString(zipInputStream)))
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

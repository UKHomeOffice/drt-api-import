package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.parser.JsonManifestParser
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger

import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.util.matching.Regex

trait ApiProviderLike {
  val log: Logger
  val dqRegex: Regex = "(drt_dq_[0-9]{6}_[0-9]{6})(_[0-9]{4}\\.zip)".r

  def filesAsSource: Source[String, NotUsed]

  def inputStream(file: String): ZipInputStream

  def filterNewer(latestFile: String)
                 (fileName: String): Boolean =
    fileName >= filterFromFileName(latestFile) && fileName != latestFile

  def filterFromFileName(latestFile: String): String = latestFile match {
    case dqRegex(dateTime, _) => dateTime
    case _ => latestFile
  }

  def manifestsStream(latestFile: String)
                     (implicit ec: ExecutionContext): Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed] = {
    log.info(s"Requesting DQ zip files > ${latestFile.take(20)}")
    val isNewer: String => Boolean = filterNewer(latestFile)
    filesAsSource
      .map { fullPath =>
        val fileName = fullPath.split("/").reverse.head
        (fullPath, fileName)
      }
      .filter { case (_, fileName) => isNewer(fileName) }
      .map { case (fullPath, fileName) =>
        val zipInputStream = inputStream(fullPath)

        log.info(s"Processing $fileName")

        (fileName, jsonsOrManifests(tryJsonContent(zipInputStream)))
      }
  }

  def jsonsOrManifests(tryJsonManifests: Try[List[(String, String)]]): Try[List[(String, Try[VoyageManifest])]] = tryJsonManifests
    .map { jsonManifests =>
      jsonManifests.map {
        case (jsonFile, json) => (jsonFile, JsonManifestParser.parseVoyagePassengerInfo(json))
      }
    }

  def tryJsonContent[X](zipInputStream: ZipInputStream): Try[List[(String, String)]] = Try {
    Stream
      .continually(zipInputStream.getNextEntry)
      .takeWhile(_ != null)
      .map { fileInZip => (fileInZip.getName, readStreamToString(zipInputStream)) }
      .toList
  }

  def readStreamToString(zipInputStream: ZipInputStream): String = {
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

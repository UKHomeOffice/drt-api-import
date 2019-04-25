package apiimport.provider

import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.stream.scaladsl.Source
import apiimport.manifests.VoyageManifestParser
import apiimport.manifests.VoyageManifestParser.VoyageManifest
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex

trait ApiProviderLike {
  val log: Logger
  val dqRegex: Regex = "(drt_dq_[0-9]{6}_[0-9]{6})(_[0-9]{4}\\.zip)".r

  def manifestsAndFailures(startingFileName: String): Source[(String, List[(String, VoyageManifest)], List[(String, String)]), NotUsed] = manifestsStream(startingFileName)
    .map {
      case (zipFileName, jsonsOrManifests) =>
        val manifests = jsonsOrManifests.collect { case Right(manifest) => manifest }
        val failedJsons = jsonsOrManifests.collect { case Left(failedJson) => failedJson }
        log.info(s"Got ${manifests.length} manifests, and ${failedJsons.length} failures")
        (zipFileName, manifests, failedJsons)
    }

  def manifestsStream(latestFile: String): Source[(String, List[Either[(String, String), (String, VoyageManifest)]]), NotUsed]

  def filterNewer(latestFile: String)(fileName: String): Boolean = fileName >= filterFromFileName(latestFile) && fileName != latestFile

  def filterFromFileName(latestFile: String): String = latestFile match {
    case dqRegex(dateTime, _) => dateTime
    case _ => latestFile
  }

  def jsonsOrManifests(jsonManifests: List[(String, String)]): List[Either[(String, String), (String, VoyageManifest)]] = jsonManifests
    .map { case (jsonFile, json) => (jsonFile, json, VoyageManifestParser.parseVoyagePassengerInfo(json)) }
    .map {
      case (jsonFile, _, Success(manifest)) => Right((jsonFile, manifest))
      case (jsonFile, json, Failure(_)) => Left((jsonFile, json))
    }

  def fileNameAndContentFromZip[X](zipFileName: String,
                                   zipInputStream: ZipInputStream): (String, List[(String, String)]) = {
    val jsonContents = tryJsonContent(zipInputStream) match {
      case Success(contents) =>
        Try(zipInputStream.close())
        contents
      case Failure(e) =>
        log.error("Failed to stream zip contents", e)
        List.empty[(String, String)]
    }

    (zipFileName, jsonContents)
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

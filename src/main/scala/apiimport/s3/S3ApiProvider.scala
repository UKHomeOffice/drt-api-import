package apiimport.s3

import java.io.InputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import apiimport.manifests.VoyageManifestParser
import apiimport.manifests.VoyageManifestParser.VoyageManifest
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.regions.Regions
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


trait ApiProviderLike {
  def manifestsStream(latestFile: String): Source[(String, List[Either[(String, String), (String, VoyageManifest)]]), NotUsed]
}

case class S3ApiProvider(awsCredentials: AWSCredentials, bucketName: String)(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) extends ApiProviderLike {
  val log = Logger(getClass)
  val dqRegex: Regex = "(drt_dq_[0-9]{6}_[0-9]{6})(_[0-9]{4}\\.zip)".r
  val config: Config = actorSystem.settings.config

  def manifestsStream(latestFile: String): Source[(String, List[Either[(String, String), (String, VoyageManifest)]]), NotUsed] = {
    log.info(s"Requesting DQ zip files > ${latestFile.take(20)}")
    zipFiles(latestFile)
      .mapAsync(1) { filename =>
        log.info(s"Fetching $filename")
        val zipByteStream = s3Client.getFileAsStream(bucketName, filename)
        Future(fileNameAndContentFromZip(filename, zipByteStream))
          .map { case (zipFileName, jsons) => (zipFileName, jsonsOrManifests(jsons)) }
      }
  }

  def jsonsOrManifests(jsonManifests: List[(String, String)]): List[Either[(String, String), (String, VoyageManifest)]] = {
    jsonManifests
      .map { case (jsonFile, json) => (jsonFile, json, VoyageManifestParser.parseVoyagePassengerInfo(json)) }
      .map {
        case (jsonFile, _, Success(manifest)) => Right((jsonFile, manifest))
        case (jsonFile, json, Failure(_)) => Left((jsonFile, json))
      }
  }

  def zipFiles(latestFile: String): Source[String, NotUsed] = {
    filterToFilesNewerThan(filesAsSource, latestFile)
  }

  def fileNameAndContentFromZip[X](zipFileName: String,
                                   zippedFileByteStream: Source[ByteString, X]): (String, List[(String, String)]) = {
    val inputStream: InputStream = zippedFileByteStream.runWith(
      StreamConverters.asInputStream()
    )
    val zipInputStream = new ZipInputStream(inputStream)

    val jsonContents = Try {
      Stream
        .continually(zipInputStream.getNextEntry)
        .takeWhile(_ != null)
        .map { fileInZip => (fileInZip.getName, readStreamToString(zipInputStream)) }
        .toList
    } match {
      case Success(contents) =>
        Try(zipInputStream.close())
        contents
      case Failure(e) =>
        log.error("Failed to stream zip contents", e)
        List.empty[(String, String)]
    }

    (zipFileName, jsonContents)
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

  def filterToFilesNewerThan(filesSource: Source[String, NotUsed], latestFile: String): Source[String, NotUsed] = {
    val filterFrom: String = filterFromFileName(latestFile)
    filesSource.filter(fn => fn >= filterFrom && fn != latestFile)
  }

  def filterFromFileName(latestFile: String): String = {
    latestFile match {
      case dqRegex(dateTime, _) => dateTime
      case _ => latestFile
    }
  }

  def filesAsSource: Source[String, NotUsed] = s3Client.
    listFilesAsStream(bucketName)//, Option("drt_dq_190417"))
    .map(_.getKey)

  def s3Client: AmazonS3Client = AmazonS3Client(Regions.EU_WEST_2, awsCredentials)()
}

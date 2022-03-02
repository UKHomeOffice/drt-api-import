package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.parser.JsonManifestParser
import advancepassengerinfo.importer.persistence.Persistence
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import drtlib.SDate
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}

import java.io.InputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


trait ManifestsProvider {
  def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed]
}

case class S3ZippedManifestsProvider(s3Client: S3AsyncClient, bucket: String)
                                    (implicit ec: ExecutionContext) extends ManifestsProvider {
  override def tryManifests(fileName: String): Source[Try[Seq[(String, Try[VoyageManifest])]], NotUsed] =
    Source
      .future(zipInputStream(fileName))
      .map(stream => Try(extractManifests(stream)))

  def zipInputStream(objectKey: String): Future[ZipInputStream] =
    inputStream(objectKey).map(s => new ZipInputStream(s))

  private def inputStream(objectKey: String): Future[InputStream] =
    s3Client
      .getObject(
        buildGetObjectRequest(objectKey),
        AsyncResponseTransformer.toBytes[GetObjectResponse]
      )
      .asScala.map(_.asInputStream())

  private def buildGetObjectRequest(objectKey: String) =
    GetObjectRequest.builder().bucket(bucket).key(objectKey).build()

  def extractManifests(zipInputStream: ZipInputStream): Seq[(String, Try[VoyageManifest])] =
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


trait DqFileProcessor {
  val process: String => Source[Option[Int], Any]
}

case class DqFileProcessorImpl(manifestsProvider: ManifestsProvider, persistence: Persistence)
                              (implicit ec: ExecutionContext) extends DqFileProcessor {
  private val log = Logger(getClass)

  val oneDayMillis: Long = 60 * 60 * 24 * 1000L
  val dqRegex: Regex = "drt_dq_([0-9]{2})([0-9]{2})([0-9]{2})_[0-9]{6}_[0-9]{4}\\.zip".r

  override val process: String => Source[Boolean, Any] = { zipFileName =>
    manifestsProvider.tryManifests(zipFileName).map {
      case Success(jsonFileNamesWithManifests) =>
        Source(jsonFileNamesWithManifests).mapAsync(1) {
          case (jsonFileName, Failure(exception)) =>
            log.error(s"Failed to extract manifest from $jsonFileName in $zipFileName: ${exception.getMessage}")
            persistence
              .persistJsonFile(zipFileName, jsonFileName, wasSuccessful = false, dateIsSuspicious = false)
              .map(_ => false)

          case (jsonFileName, Success(manifest)) =>
            persistence
              .persistManifest(jsonFileName, manifest)
              .flatMap {
                case Some(rowCount) =>
                  val isSuspicious = scheduledIsSuspicious(zipFileName, manifest)
                  persistence
                    .persistJsonFile(zipFileName, jsonFileName, rowCount > 1, isSuspicious)
                    .map(_ => true)
                case None =>
                  persistence
                    .persistJsonFile(zipFileName, jsonFileName, wasSuccessful = false, dateIsSuspicious = false)
                    .map(_ => false)
              }
        }
      case Failure(throwable) =>
        log.error(s"Failed to process zip file $zipFileName: ${throwable.getMessage}")
        None
    }
  }

  def scheduledIsSuspicious(zf: String, vm: VoyageManifest): Boolean = {
    val maybeSuspiciousDate: Option[Boolean] = for {
      zipDate <- zipFileDate(zf)
      scdDate <- vm.scheduleArrivalDateTime
    } yield {
      scdDate.millisSinceEpoch - zipDate.millisSinceEpoch > 2 * oneDayMillis
    }

    maybeSuspiciousDate.getOrElse(false)
  }

  def zipFileDate(fileName: String): Option[SDate] = fileName match {
    case dqRegex(year, month, day) => Option(SDate(s"20$year-$month-$day"))
    case _ => None
  }
}

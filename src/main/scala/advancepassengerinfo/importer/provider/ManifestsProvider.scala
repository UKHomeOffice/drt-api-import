package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.parser.JsonManifestParser
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.stream.scaladsl.Source
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}

import java.io.InputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.util.zip.ZipInputStream
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps
import scala.util.Try

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

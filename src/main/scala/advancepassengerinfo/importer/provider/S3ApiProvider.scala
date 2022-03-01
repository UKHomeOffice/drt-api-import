package advancepassengerinfo.importer.provider

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, ListObjectsV2Request}

import java.io.InputStream
import java.util.zip.ZipInputStream
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps


case class S3ApiProvider(awsCredentials: AwsBasicCredentials, bucketName: String, filesPrefix: String)
                        (implicit executionContext: ExecutionContext) extends ApiProviderLike {
  val log: Logger = Logger(getClass)

  val staticCreds: StaticCredentialsProvider = StaticCredentialsProvider.create(awsCredentials)

  val s3Client: S3AsyncClient = S3AsyncClient.builder()
    .credentialsProvider(staticCreds)
    .build()

  def filesAsSource: Source[String, NotUsed] = Source(List())

  def downloadAsInputStream(objectKey: String): Future[InputStream] = s3Client
    .getObject(
      getObjectRequest(objectKey),
      AsyncResponseTransformer.toBytes[GetObjectResponse]
    )
    .asScala.map(_.asInputStream())

  ////////// to do ////////////
  def getFiles = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build()).asScala.map { res =>
    res.contents().asScala.map(_.key())
  }

  private def getObjectRequest(objectKey: String) =
    GetObjectRequest.builder().bucket(bucketName).key(objectKey).build()

  def inputStream(objectKey: String): ZipInputStream = {
    val zipByteStream: Future[InputStream] = s3Client
      .getObject(
        getObjectRequest(objectKey),
        AsyncResponseTransformer.toBytes[GetObjectResponse]
      )
      .asScala.map(_.asInputStream())
    val inputStream: InputStream = Await.result(zipByteStream, 30.seconds)
    new ZipInputStream(inputStream)
  }
}

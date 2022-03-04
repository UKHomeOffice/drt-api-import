package advancepassengerinfo.importer.provider

import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}

import java.io.InputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.CompletionStageOps

trait FileAsStream {
  def asInputStream(objectKey: String): Future[InputStream]
}

case class S3FileAsStream(s3Client: S3AsyncClient, bucket: String)
                         (implicit ec: ExecutionContext) extends FileAsStream {
  override def asInputStream(objectKey: String): Future[InputStream] = s3Client.getObject(
    GetObjectRequest.builder().bucket(bucket).key(objectKey).build(),
    AsyncResponseTransformer.toBytes[GetObjectResponse]
  ).asScala.map(_.asInputStream())
}

package advancepassengerinfo.importer.provider

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps

trait FileNames {
  val nextFiles: String => Future[List[String]]
}

case class S3FileNames(s3Client: S3AsyncClient, bucket: String)
                      (implicit ec: ExecutionContext) extends FileNames {
  override val nextFiles: String => Future[List[String]] = (lastFile: String) => s3Client
    .listObjects(ListObjectsRequest.builder().bucket(bucket).maxKeys(100).marker(lastFile).build()).asScala
    .map(_.contents().asScala.map(_.key()).toList)
}

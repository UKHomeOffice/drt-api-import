package advancepassengerinfo.importer.provider

import com.typesafe.scalalogging.Logger
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.ListObjectsRequest

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.FutureConverters.CompletionStageOps

trait FileNames {
  val nextFiles: (String, String) => Future[List[String]]

  val s3Files: String => Future[List[String]]
}

case class S3FileNames(s3Client: S3AsyncClient, bucket: String)
                      (implicit ec: ExecutionContext) extends FileNames {
  private val log = Logger(getClass)

  val s3Files: String => Future[List[String]] = lastFile => s3Client
    .listObjects(ListObjectsRequest.builder().bucket(bucket).marker(lastFile).build()).asScala
    .map(_.contents().asScala.map(_.key()).toList)

  override val nextFiles: (String, String) => Future[List[String]] = (lastFile: String, fallbackFileName) => s3Files(lastFile)
    .recoverWith {
      case _ =>
        log.error(s"Failed to get next files after $lastFile. Falling back to $fallbackFileName")
        s3Files(fallbackFileName)
    }
}

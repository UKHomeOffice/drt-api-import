package advancepassengerinfo.importer.provider

import java.io.InputStream
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, StreamConverters}
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.regions.Regions
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionContext


case class S3ApiProvider(awsCredentials: AWSCredentials, bucketName: String)(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) extends ApiProviderLike {
  val log = Logger(getClass)
  val config: Config = actorSystem.settings.config

  def filesAsSource: Source[String, NotUsed] = s3Client.
    listFilesAsStream(bucketName) //, Option("drt_dq_190417"))
    .map(_.getKey)

  def inputStream(fileName: String): ZipInputStream = {
    val zipByteStream = s3Client.getFileAsStream(bucketName, fileName)
    val inputStream: InputStream = zipByteStream.runWith(StreamConverters.asInputStream())
    new ZipInputStream(inputStream)
  }

  def s3Client: AmazonS3Client = AmazonS3Client(Regions.EU_WEST_2, awsCredentials)()
}

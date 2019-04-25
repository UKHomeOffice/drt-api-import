package apiimport.provider

import java.io.InputStream
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, StreamConverters}
import akka.util.ByteString
import apiimport.manifests.VoyageManifestParser.VoyageManifest
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.regions.Regions
import com.mfglabs.commons.aws.s3.AmazonS3Client
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}


case class S3ApiProvider(awsCredentials: AWSCredentials, bucketName: String)(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) extends ApiProviderLike {
  val log = Logger(getClass)
  val config: Config = actorSystem.settings.config

  def manifestsStream(latestFile: String): Source[(String, List[Either[(String, String), (String, VoyageManifest)]]), NotUsed] = {
    log.info(s"Requesting DQ zip files > ${latestFile.take(20)}")
    filesAsSource
      .filter(filterNewer(latestFile))
      .mapAsync(1) { filename =>
        log.info(s"Fetching $filename")
        val zipByteStream = s3Client.getFileAsStream(bucketName, filename)
        val zipInputStream: ZipInputStream = inputStream(zipByteStream)

        Future(fileNameAndContentFromZip(filename, zipInputStream))
          .map { case (zipFileName, jsons) => (zipFileName, jsonsOrManifests(jsons)) }
      }
  }

  def inputStream[X](zippedFileByteStream: Source[ByteString, X]): ZipInputStream = {
    val inputStream: InputStream = zippedFileByteStream.runWith(
      StreamConverters.asInputStream()
    )
    val zipInputStream = new ZipInputStream(inputStream)
    zipInputStream
  }

  def filesAsSource: Source[String, NotUsed] = s3Client.
    listFilesAsStream(bucketName) //, Option("drt_dq_190417"))
    .map(_.getKey)

  def s3Client: AmazonS3Client = AmazonS3Client(Regions.EU_WEST_2, awsCredentials)()
}

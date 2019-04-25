package apiimport

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import apiimport.persistence.ManifestPersistor
import apiimport.provider.LocalApiProvider
import com.amazonaws.auth.AWSCredentials
import com.typesafe.config.ConfigFactory
import slick.jdbc.PostgresProfile
import slickdb.Tables

import scala.concurrent.ExecutionContext

object PostgresTables extends {
  val profile = PostgresProfile
} with Tables

object Main extends App {
  println("Hello!")

  val config = ConfigFactory.load

  val bucketName = config.getString("s3.api-data.bucket-name")
  val awsCredentials: AWSCredentials = new AWSCredentials {
    override def getAWSAccessKeyId: String = config.getString("s3.api-data.credentials.access_key_id")

    override def getAWSSecretKey: String = config.getString("s3.api-data.credentials.secret_key")
  }

  implicit val actorSystem = ActorSystem("api-data-import")
  implicit val ec = ExecutionContext.global
  implicit val materializer = ActorMaterializer()

//  val poller = new S3ManifestPoller("BRS", "drt_dq_190417", S3ApiProvider(awsCredentials, bucketName))
  val poller = new ManifestPoller(LocalApiProvider("/home/rich/drt-s3-backup"), ManifestPersistor(PostgresTables))

  poller.startPollingForManifests()
}

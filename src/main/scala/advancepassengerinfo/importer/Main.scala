package advancepassengerinfo.importer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import advancepassengerinfo.importer.PostgresTables.profile
import advancepassengerinfo.importer.persistence.ManifestPersistor
import advancepassengerinfo.importer.provider.{ApiProviderLike, LocalApiProvider, S3ApiProvider}
import com.amazonaws.auth.AWSCredentials
import com.typesafe.config.ConfigFactory
import slick.jdbc.PostgresProfile
import slickdb.Tables

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object PostgresTables extends {
  val profile = PostgresProfile
} with Tables

object Main extends App {
  val config = ConfigFactory.load

  implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  def providerFromConfig(localImportPath: String): ApiProviderLike = {
    if (localImportPath.nonEmpty)
      LocalApiProvider(localImportPath)
    else {
      val bucketName = config.getString("s3.api-data.bucket-name")
      val awsCredentials: AWSCredentials = new AWSCredentials {
        override def getAWSAccessKeyId: String = config.getString("s3.api-data.credentials.access_key_id")

        override def getAWSSecretKey: String = config.getString("s3.api-data.credentials.secret_key")
      }

      S3ApiProvider(awsCredentials, bucketName)
    }
  }

  val localImportPath = config.getString("local-import-path")

  val provider = providerFromConfig(localImportPath)
  val persistor = ManifestPersistor(PostgresDb)

  val poller = new ManifestPoller(provider, persistor)

  poller.startPollingForManifests()
}

trait Db {
  val tables: advancepassengerinfo.importer.slickdb.Tables
  val con: tables.profile.backend.Database
}

object PostgresDb extends Db {
  val tables: PostgresTables.type = PostgresTables
  val con: profile.backend.Database = tables.profile.backend.Database.forConfig("db")
}

package advancepassengerinfo.importer

import java.util.TimeZone

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

  def defaultTimeZone: String = TimeZone.getDefault.getID

  def systemTimeZone: String = System.getProperty("user.timezone")

  assert(systemTimeZone == "UTC", "System Timezone is not set to UTC")
  assert(defaultTimeZone == "UTC", "Default Timezone is not set to UTC")

  def providerFromConfig(localImportPath: String): ApiProviderLike = {
    if (localImportPath.nonEmpty)
      LocalApiProvider(localImportPath)
    else {
      val bucketName = config.getString("s3.api-data.bucket-name")
      val filesPrefix = config.getString("s3.api-data.files_prefix")
      val awsCredentials: AWSCredentials = new AWSCredentials {
        override def getAWSAccessKeyId: String = config.getString("s3.api-data.credentials.access_key_id")

        override def getAWSSecretKey: String = config.getString("s3.api-data.credentials.secret_key")
      }

      S3ApiProvider(awsCredentials, bucketName, filesPrefix)
    }
  }

  val localImportPath = config.getString("local-import-path")

  val provider = providerFromConfig(localImportPath)
  val parallelism = config.getInt("parallelism")
  val persistor = ManifestPersistor(PostgresDb, parallelism)

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

package advancepassengerinfo.importer

import advancepassengerinfo.importer.PostgresTables.profile
import advancepassengerinfo.importer.persistence.PersistenceImp
import advancepassengerinfo.importer.provider._
import advancepassengerinfo.importer.slickdb.Tables
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import slick.jdbc.PostgresProfile
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.util.TimeZone
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}

object PostgresTables extends {
  val profile = PostgresProfile
} with Tables

object Main extends App {
  val config = ConfigFactory.load

  implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def defaultTimeZone: String = TimeZone.getDefault.getID

  def systemTimeZone: String = System.getProperty("user.timezone")

  assert(systemTimeZone == "UTC", "System Timezone is not set to UTC")
  assert(defaultTimeZone == "UTC", "Default Timezone is not set to UTC")

  private val bucketName = config.getString("s3.api-data.bucket-name")

  private def s3Client: S3AsyncClient = {
    val accessKey = config.getString("s3.api-data.credentials.access_key_id")
    val secretKey = config.getString("s3.api-data.credentials.secret_key")
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

    S3AsyncClient.builder()
      .credentialsProvider(credentialsProvider)
      .build()
  }

  val s3FileNamesProvider = S3FileNamesProviderImpl(s3Client, bucketName)
  val fileNamesProvider = DqFileNameProvider(s3FileNamesProvider)
  val manifestsProvider = S3ZippedManifestsProvider(s3Client, bucketName)
  val persistence = PersistenceImp(PostgresDb)
  val feed = DqApiFeedImpl(fileNamesProvider, DqFileProcessorImpl(manifestsProvider, persistence), 1.second)
  val eventual = persistence.lastPersistedFileName.map {
    case Some(lastFileName) => feed.processFilesAfter(lastFileName)
    case None => feed.processFilesAfter("")
  }

  Await.ready(eventual, Duration.Inf)
}

trait Db {
  val tables: advancepassengerinfo.importer.slickdb.Tables
  val con: tables.profile.backend.Database
}

object PostgresDb extends Db {
  val tables: PostgresTables.type = PostgresTables
  val con: profile.backend.Database = tables.profile.backend.Database.forConfig("db")
}

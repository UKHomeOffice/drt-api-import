package advancepassengerinfo.importer

import advancepassengerinfo.importer.PostgresTables.profile
import advancepassengerinfo.importer.persistence.DbPersistenceImpl
import advancepassengerinfo.importer.processor.DqFileProcessorImpl
import advancepassengerinfo.importer.provider._
import advancepassengerinfo.importer.slickdb.Tables
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import drtlib.SDate
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
  val log = Logger(getClass)
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

  val s3FileNamesProvider = S3FileNames(s3Client, bucketName)
  val s3FileAsStream = S3FileAsStream(s3Client, bucketName)
  val manifestsProvider = ZippedManifests(s3FileAsStream)
  val persistence = DbPersistenceImpl(PostgresDb)
  val zipProcessor = DqFileProcessorImpl(manifestsProvider, persistence)
  val feed = DqApiFeedImpl(s3FileNamesProvider, zipProcessor, 1.minute)

  val eventual = Source
    .future(persistence.lastPersistedFileName)
    .log("manifests")
    .flatMapConcat {
      case Some(lastFileName) =>
        log.info(s"Last processed file: $lastFileName")
        feed.processFilesAfter(lastFileName)
      case None =>
        val date = SDate.now().addDays(-2)
        val yymmdd = f"${date.getFullYear() - 2000}${date.getMonth()}%02d${date.getDate()}%02d"
        log.info(s"No last processed file. Starting from 2 days ago ($yymmdd)")
        feed.processFilesAfter("drt_dq_" + yymmdd + "_000000_0000.zip")
    }.runWith(Sink.ignore)

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


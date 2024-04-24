package advancepassengerinfo.importer

import advancepassengerinfo.health.{HealthRoute, LastCheckedState}
import advancepassengerinfo.importer.processor.DqFileProcessorImpl
import advancepassengerinfo.importer.provider._
import advancepassengerinfo.importer.services.Retention
import advancepassengerinfo.importer.slickdb.DatabaseImpl
import advancepassengerinfo.importer.slickdb.dao.{DataRetentionDao, ProcessedJsonDaoImpl, ProcessedZipDaoImpl, VoyageManifestPassengerInfoDaoImpl}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import drtlib.SDate
import metrics.StatsDMetrics
import slick.dbio.{DBIOAction, NoStream}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.util.TimeZone
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try


object Application extends App {
  val log = Logger(getClass)
  val config = ConfigFactory.load

  implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  private def defaultTimeZone: String = TimeZone.getDefault.getID

  private def systemTimeZone: String = System.getProperty("user.timezone")

  assert(systemTimeZone == "UTC", "System Timezone is not set to UTC")
  assert(defaultTimeZone == "UTC", "Default Timezone is not set to UTC")

  private val bucketName = config.getString("s3.api-data.bucket-name")
  private val retainDataForYears = config.getInt("app.retain-data-for-years")

  val lastCheckedState = LastCheckedState(() => SDate.now())

  private def s3Client: S3AsyncClient = {
    val accessKey = config.getString("s3.api-data.credentials.access_key_id")
    val secretKey = config.getString("s3.api-data.credentials.secret_key")
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

    S3AsyncClient.builder()
      .credentialsProvider(credentialsProvider)
      .region(Region.EU_WEST_2)
      .build()
  }

  private val s3FileNamesProvider = S3FileNames(s3Client, bucketName)
  private val s3FileAsStream = S3FileAsStream(s3Client, bucketName)
  private val manifestsProvider = ZippedManifests(s3FileAsStream)
  private val zipDao = ProcessedZipDaoImpl(PostgresDb)
  private val jsonDao = ProcessedJsonDaoImpl(PostgresDb)
  private val manifestsDao = VoyageManifestPassengerInfoDaoImpl(PostgresDb)
  private val retentionDao = DataRetentionDao(PostgresDb)
  private val zipProcessor = DqFileProcessorImpl(manifestsProvider, zipDao, jsonDao, manifestsDao)
  private val feed = DqApiFeedImpl(s3FileNamesProvider, zipProcessor, 1.minute, StatsDMetrics, lastCheckedState)

  Http().newServerAt(config.getString("server.host"), config.getInt("server.port")).bind(HealthRoute(lastCheckedState, 5.minutes))

  private val eventual = Source
    .future(zipDao.lastPersistedFileName)
    .recover {
      case t =>
        log.error(s"Failed to get last persisted file name: ${t.getMessage}")
        None
    }
    .log("manifests")
    .flatMapConcat {
      case Some(lastFileName) =>
        log.info(s"Last processed file: $lastFileName")
        feed.processFilesAfter(lastFileName)
      case None =>
        val date = SDate.now().plus(2.days)
        val yyyymmdd: String = date.toYyyyMMdd
        val lastFilename = "drt_dq_" + yyyymmdd + "_000000_0000.zip"
        log.info(s"No last processed file. Starting from 2 days ago ($yyyymmdd)")
        feed.processFilesAfter(lastFilename)
    }.runWith(Sink.ignore)

  val isBeyondRetentionPeriod = Retention.isOlderThanRetentionThreshold(retainDataForYears, SDate.now)
  val oldestData = () => zipDao.oldestDate.map { md =>
    md
      .flatMap(d => Try(SDate(d)).toOption)
      .filter(isBeyondRetentionPeriod)
  }
  val deleteOldData = Retention.deleteOldData(oldestData, retentionDao.deleteForDate)

  actorSystem.scheduler.scheduleAtFixedRate(0.seconds, 1.minute)(() => deleteOldData())
  actorSystem.scheduler.scheduleAtFixedRate(0.seconds, 1.minute) { () =>
    val retentionStartDate = SDate.now().minus((retainDataForYears * 365).days).millisSinceEpoch
    jsonDao.earliestUnpopulatedDate(retentionStartDate).map {
      _.map { date =>
        log.info(s"Populating earliest unpopulated date: $date")
        jsonDao.populateManifestColumnsForDate(date)
      }
    }
  }

  Await.ready(eventual, Duration.Inf)
}

trait Db {
  val profile: slick.jdbc.JdbcProfile
  protected val con: profile.backend.Database

  def run[T]: DBIOAction[T, NoStream, Nothing] => Future[T] = con.run
}

object PostgresDb extends Db {
  override val profile = DatabaseImpl.profile
  val con: profile.backend.Database = profile.api.Database.forConfig("db")
}


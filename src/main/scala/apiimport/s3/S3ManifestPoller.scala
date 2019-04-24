package apiimport.s3

import java.sql.Timestamp

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable, Scheduler}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import apiimport.manifests.VoyageManifestParser.VoyageManifest
import drtlib.SDate
import org.slf4j.{Logger, LoggerFactory}
import slickdb.PostgresTables
import slickdb.PostgresTables.profile

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex

class S3ManifestPoller(portCode: String, initialLastSeenFileName: String, provider: ApiProviderLike)
                      (implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) {

  val log: Logger = LoggerFactory.getLogger(getClass)

  import slickdb.VoyageManifestPassengerInfoTable

  val vmTable = VoyageManifestPassengerInfoTable(PostgresTables)
  val db: profile.backend.Database = PostgresTables.profile.api.Database.forConfig("db")

  val dqRegex: Regex = "(drt_dq_[0-9]{6}_[0-9]{6})(_[0-9]{4}\\.zip)".r

  var lastSeenFileName: String = initialLastSeenFileName
  var lastFetchedMillis: Long = 0

  def newManifestsStream(startingFileName: String): Source[(String, List[(String, VoyageManifest)], List[(String, String)]), NotUsed] = {
    log.info(s"Fetching manifests from files newer than $startingFileName")
    provider
      .manifestsStream(startingFileName)
      .map {
        case (zipFileName, jsonsOrManifests) =>
          val manifests = jsonsOrManifests.collect { case Right(manifest) => manifest }
          val failedJsons = jsonsOrManifests.collect { case Left(failedJson) => failedJson }
          log.info(s"Got ${manifests.length} manifests, and ${failedJsons.length} failures")
          (zipFileName, manifests, failedJsons)
      }
  }

  def startPollingForManifests(): Cancellable = {
    actorSystem.scheduler.schedule(0 seconds, 5 minute, new Runnable {
      def run(): Unit = {
        implicit val scheduler: Scheduler = actorSystem.scheduler
        newManifestsStream(lastSeenFileName)
          .mapAsync(1) {
            case (zipFileName, manifests, failedJsons) => insertManifests(manifests, zipFileName)
          }
          .runWith(Sink.seq)
      }
    })
  }

  def insertManifests(manifests: List[(String, VoyageManifest)], zipFileName: String): Future[List[Any]] = {
    val futureManifests = manifests.map { case (jsonFile, vm) =>
      val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

      val eventualDowWoy: Future[Option[(Int, Int)]] = db.run(vmTable.dayOfWeekAndWeekOfYear(schTs))

      eventualDowWoy.flatMap {
        case Some((dayOfWeek, weekOfYear)) =>
          log.info(s"Trying to insert vm from $zipFileName with ${vm.PassengerList.length} passengers into db")
          val rows = vmTable.rowsToInsert(vm, dayOfWeek, weekOfYear, jsonFile)
          val eventualResult = db.run(rows)
          eventualResult
        case _ =>
          log.error(s"Didn't manage to get dayOfWeek or weekOfYear for ${SDate(schTs.getTime).toISOString()}")
          Future(Unit)
      }.recover {
        case t =>
          log.error("Failed to insert manifest passengers", t)
          Unit
      }
    }
    Future.sequence(futureManifests)
  }

//  def insertFailedManifests(manifests: List[String], zipFileName: String): Future[List[Any]] = {
//    val futureManifests = manifests.map { vm =>
//      log.info(s"Trying to insert failed manifests json from $zipFileName into db")
//      val rows = vmTable.rowsToInsert(vm, dayOfWeek, weekOfYear)
//      val eventualResult = db.run(rows)
//      eventualResult
//    }
//    Future.sequence(futureManifests)
//  }
}

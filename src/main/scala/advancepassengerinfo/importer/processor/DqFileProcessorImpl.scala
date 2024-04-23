package advancepassengerinfo.importer.processor

import advancepassengerinfo.importer.provider.Manifests
import advancepassengerinfo.importer.slickdb.dao.{ProcessedJsonDao, ProcessedZipDao, VoyageManifestPassengerInfoDao}
import advancepassengerinfo.importer.slickdb.serialisation.VoyageManifestSerialisation.voyageManifestRows
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonRow, ProcessedZipRow}
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import drtlib.SDate

import java.sql.Timestamp
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


trait DqFileProcessor {
  def process(zipFileName: String): Source[Option[(Int, Int)], Any]
}

case class DqFileProcessorImpl(manifestsProvider: Manifests,
                               zipDao: ProcessedZipDao,
                               jsonDao: ProcessedJsonDao,
                               manifestsDao: VoyageManifestPassengerInfoDao,
                              )
                              (implicit ec: ExecutionContext) extends DqFileProcessor {
  private val log = Logger(getClass)

  private val oneDayMillis: Long = 1.day.toMillis

  def process(zipFileName: String): Source[Option[(Int, Int)], Any] = {
    val processedAtTs = new Timestamp(SDate.now().millisSinceEpoch)
    val maybeCreatedOn = ProcessedZipRow.extractCreatedOn(zipFileName)

    manifestsProvider.tryManifests(zipFileName).flatMapConcat {
        case Success(jsonFileNamesWithManifests) =>
          persistManifests(zipFileName, jsonFileNamesWithManifests)
            .mapAsync(1) { case (total, successful) =>
              val row = ProcessedZipRow(zipFileName, successful > 0, processedAtTs, maybeCreatedOn)
              zipDao.insert(row).map(_ => Option((total, successful)))
            }
            .recover {
              case t =>
                log.error(s"Failed to persist zip file $zipFileName: ${t.getMessage}")
                None
            }

        case Failure(throwable) =>
          log.error(s"Failed to process zip file $zipFileName: ${throwable.getMessage}")
          val row = ProcessedZipRow(zipFileName, success = false, processedAtTs, maybeCreatedOn)
          Source
            .future(zipDao.insert(row))
            .map(_ => None)
            .recover {
              case t =>
                log.error(s"Failed to persist zip file $zipFileName: ${t.getMessage}")
                None
            }
      }
      .recover {
        case t =>
          log.error(s"Failed to process files after $zipFileName: ${t.getMessage}")
          None
      }
  }

  private def persistManifests(zipFileName: String, manifestTries: Seq[(String, Try[VoyageManifest])]): Source[(Int, Int), NotUsed] =
    Source(manifestTries)
      .foldAsync((0, 0)) {
        case ((total, success), (jsonFileName, tryManifest)) =>
          val processedAt = SDate.now().millisSinceEpoch
          jsonDao.jsonHasBeenProcessed(zipFileName, jsonFileName).flatMap { alreadyProcessed =>
              (alreadyProcessed, tryManifest) match {
                case (true, _) =>
                  log.warn(s"Skipping $jsonFileName as it's already been processed as part of $zipFileName")
                  Future.successful((total, success))
                case (false, Failure(exception)) =>
                  log.error(s"Failed to extract manifest from $jsonFileName in $zipFileName: ${exception.getMessage}")
                  persistFailedJson(zipFileName, jsonFileName, processedAt).map(_ => (total + 1, success))

                case (false, Success(manifest)) =>
                  persistManifest(zipFileName, total, success, jsonFileName, processedAt, manifest)
              }
            }
            .recover {
              case t =>
                log.error(s"Failed to persist manifest from $jsonFileName in $zipFileName: ${t.getMessage}")
                (total + 1, success)
            }
      }

  private def persistManifest(zipFileName: String,
                              total: Int,
                              success: Int,
                              jsonFileName: String,
                              processedAt: Long,
                              manifest: VoyageManifest,
                             ): Future[(Int, Int)] = {
    manifest.scheduleArrivalDateTime match {
      case Some(scheduled) =>
        manifestsDao.dayOfWeekAndWeekOfYear(new Timestamp(scheduled.millisSinceEpoch))
          .flatMap {
            case (dayOfWeek, weekOfYear) =>
              val manifestRows = voyageManifestRows(manifest, dayOfWeek, weekOfYear, jsonFileName)
              manifestsDao.insert(manifestRows)
          }
          .flatMap { _ =>
            persistSuccessfulJson(zipFileName, jsonFileName, manifest, processedAt).map(_ => (total + 1, success + 1))
          }
          .recoverWith {
            case t =>
              log.error(s"Failed to persist manifest from $jsonFileName in $zipFileName: ${t.getMessage}")
              persistFailedJson(zipFileName, jsonFileName, processedAt).map(_ => (total + 1, success))
          }
      case None =>
        log.error(s"Failed to get a scheduled time for ${manifest.DeparturePortCode} > ${manifest.ArrivalPortCode} " +
          s":: ${manifest.CarrierCode}-${manifest.VoyageNumber} :: ${manifest.ScheduledDateOfArrival}T${manifest.ScheduledDateOfArrival}")
        persistFailedJson(zipFileName, jsonFileName, processedAt).map(_ => (total + 1, success))
    }
  }

  private def persistSuccessfulJson(zipFileName: String, jsonFileName: String, manifest: VoyageManifest, processedAt: Long): Future[Unit] = {
    val isSuspicious = scheduledIsSuspicious(zipFileName, manifest)
    val row = ProcessedJsonRow.fromManifest(zipFileName, jsonFileName, successful = true, dateIsSuspicious = isSuspicious, Option(manifest), processedAt)
    jsonDao.insert(row)
  }

  private def persistFailedJson(zipFileName: String, jsonFileName: String, processedAt: Long): Future[Unit] = {
    val row = ProcessedJsonRow.fromManifest(zipFileName, jsonFileName, successful = false, dateIsSuspicious = false, None, processedAt)
    jsonDao.insert(row)
  }


  private def scheduledIsSuspicious(zf: String, vm: VoyageManifest): Boolean = {
    val maybeSuspiciousDate: Option[Boolean] = for {
      zipDate <- ProcessedZipRow.extractCreatedOn(zf)
      scdDate <- vm.scheduleArrivalDateTime
    } yield {
      scdDate.millisSinceEpoch - SDate(zipDate).millisSinceEpoch > 2 * oneDayMillis
    }

    maybeSuspiciousDate.getOrElse(false)
  }
}


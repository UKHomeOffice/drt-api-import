package advancepassengerinfo.importer.processor

import advancepassengerinfo.importer.persistence.Persistence
import advancepassengerinfo.importer.provider.Manifests
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import drtlib.SDate

import scala.concurrent.{ExecutionContext, Future}
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

trait DqFileProcessor {
  def process(zipFileName: String): Source[Option[(Int, Int)], Any]
}

case class DqFileProcessorImpl(manifestsProvider: Manifests, persistence: Persistence)
                              (implicit ec: ExecutionContext) extends DqFileProcessor {
  private val log = Logger(getClass)

  val oneDayMillis: Long = 60 * 60 * 24 * 1000L
  val dqFileNameDateRegex: Regex = "drt_dq_([0-9]{2})([0-9]{2})([0-9]{2})_[0-9]{6}_[0-9]{4}\\.zip".r

  def process(zipFileName: String): Source[Option[(Int, Int)], Any] = {
    manifestsProvider.tryManifests(zipFileName).flatMapConcat {
      case Success(jsonFileNamesWithManifests) =>
        persistManifests(zipFileName, jsonFileNamesWithManifests)
          .mapAsync(1) { case (total, successful) =>
            persistence
              .persistZipFile(zipFileName, successful > 0)
              .map(_ => Option((total, successful)))
          }

      case Failure(throwable) =>
        log.error(s"Failed to process zip file $zipFileName: ${throwable.getMessage}")
        Source.future(persistence
          .persistZipFile(zipFileName, successful = false)
          .map(_ => None))
    }
  }

  private def persistManifests(zipFileName: String, manifestTries: Seq[(String, Try[VoyageManifest])]): Source[(Int, Int), NotUsed] =
    Source(manifestTries)
      .foldAsync((0, 0)) {
        case ((total, success), (jsonFileName, tryManifest)) =>
          persistence.jsonHasBeenProcessed(zipFileName, jsonFileName).flatMap { alreadyProcessed =>
            (alreadyProcessed, tryManifest) match {
              case (true, _) =>
                log.warn(s"Skipping $jsonFileName as it's already been processed as part of $zipFileName")
                Future.successful((total, success))
              case (false, Failure(exception)) =>
                log.error(s"Failed to extract manifest from $jsonFileName in $zipFileName: ${exception.getMessage}")
                persistFailedJson(zipFileName, jsonFileName).map(_ => (total + 1, success))

              case (false, Success(manifest)) =>
                persistence
                  .persistManifest(jsonFileName, manifest)
                  .flatMap {
                    case Some(_) =>
                      persistSuccessfulJson(zipFileName, jsonFileName, manifest).map(_ => (total + 1, success + 1))
                    case None =>
                      persistFailedJson(zipFileName, jsonFileName).map(_ => (total + 1, success))
                  }
            }
          }
      }

  private def persistSuccessfulJson(zipFileName: String, jsonFileName: String, manifest: VoyageManifest): Future[Int] = {
    val isSuspicious = scheduledIsSuspicious(zipFileName, manifest)
    persistence.persistJsonFile(zipFileName, jsonFileName, successful = true, dateIsSuspicious = isSuspicious)
  }

  private def persistFailedJson(zipFileName: String, jsonFileName: String): Future[Int] =
    persistence.persistJsonFile(zipFileName, jsonFileName, successful = false, dateIsSuspicious = false)

  private def scheduledIsSuspicious(zf: String, vm: VoyageManifest): Boolean = {
    val maybeSuspiciousDate: Option[Boolean] = for {
      zipDate <- zipFileDate(zf)
      scdDate <- vm.scheduleArrivalDateTime
    } yield {
      scdDate.millisSinceEpoch - zipDate.millisSinceEpoch > 2 * oneDayMillis
    }

    maybeSuspiciousDate.getOrElse(false)
  }

  private def zipFileDate(fileName: String): Option[SDate] = fileName match {
    case dqFileNameDateRegex(year, month, day) => Option(SDate(s"20$year-$month-$day"))
    case _ => None
  }
}

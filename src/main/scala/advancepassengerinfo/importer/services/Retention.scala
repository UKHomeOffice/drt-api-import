package advancepassengerinfo.importer.services

import drtlib.SDate
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object Retention {
  def isOlderThanRetentionThreshold(retentionYears: Int, now: () => SDate): SDate => Boolean =
    _ < now().minus(365 * retentionYears.days)

  def deleteOldData(maybeDeletableDate: () => Future[Option[SDate]],
                    deleteForDate: SDate => Future[(Int, Int, Int)],
                   )
                   (implicit ec: ExecutionContext): () => Future[(Int, Int, Int)] = {
    val log = LoggerFactory.getLogger(getClass)

    () => maybeDeletableDate().flatMap {
      case Some(date) =>
        val start = System.currentTimeMillis()
        val eventualTuple = deleteForDate(date)
        eventualTuple.onComplete {
          case Success((deletedZips, deletedJsons, deletedManifests)) =>
            log.info(s"Deleted $deletedZips zips, $deletedJsons jsons, $deletedManifests manifests. Took ${System.currentTimeMillis() - start}ms")
          case Failure(exception) =>
            log.error(s"Failed to delete data: ${exception.getMessage}")
        }
        eventualTuple
      case _ =>
        log.info("No data to delete")
        Future.successful((0, 0, 0))
    }
  }
}

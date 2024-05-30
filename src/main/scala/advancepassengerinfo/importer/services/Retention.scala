package advancepassengerinfo.importer.services

import akka.actor.ActorSystem
import drtlib.SDate
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success}

object Retention {
  def isOlderThanRetentionThreshold(retentionYears: Int, now: () => SDate): SDate => Boolean = {
    val daysToRetain = 365 * retentionYears
    dateToConsider => dateToConsider < now().minus(daysToRetain.days)
  }

  def deleteOldData(maybeDeletableDate: () => Future[Option[SDate]],
                    deleteForDate: SDate => Future[(Int, Int, Int)],
                   )
                   (implicit system: ActorSystem, exc: ExecutionContext): () => Future[(Int, Int, Int)] = {
    val log = LoggerFactory.getLogger(getClass)

    () =>
      maybeDeletableDate().flatMap {
        case Some(date) =>
          log.info(s"Deleting data for $date")
          val start = System.currentTimeMillis()
          val eventualTuple = deleteForDate(date)
          eventualTuple.onComplete {
            case Success((deletedManifests, deletedJsons, deletedZips)) =>
              log.info(s"Deleted $deletedZips zips, $deletedJsons jsons, $deletedManifests manifests. Took ${System.currentTimeMillis() - start}ms")
              system.scheduler.scheduleOnce(1.minute)(deleteOldData(maybeDeletableDate, deleteForDate))
            case Failure(exception) =>
              log.error(s"Failed to delete data: ${exception.getMessage}")
              system.scheduler.scheduleOnce(30.minute)(deleteOldData(maybeDeletableDate, deleteForDate))
          }
          eventualTuple
        case _ =>
          Future.successful((0, 0, 0))
      }
  }
}

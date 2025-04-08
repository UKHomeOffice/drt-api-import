package advancepassengerinfo.importer

import advancepassengerinfo.health.LastCheckedState
import advancepassengerinfo.importer.processor.DqFileProcessor
import advancepassengerinfo.importer.provider.FileNames
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import drtlib.SDate
import metrics.MetricsCollectorLike

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait DqApiFeed {
  def processFilesAfter(lastFileName: String): Source[String, NotUsed]
}

case class DqApiFeedImpl(fileNamesProvider: FileNames,
                         fileProcessor: DqFileProcessor,
                         throttle: FiniteDuration,
                         metricsCollector: MetricsCollectorLike,
                         lastCheckedState: LastCheckedState)
                        (implicit ec: ExecutionContext) extends DqApiFeed {
  private val log = Logger(getClass)

  override def processFilesAfter(lastFileName: String): Source[String, NotUsed] =
    Source
      .unfoldAsync((lastFileName, List[String]())) { case (lastFileName, lastFiles) =>
        markerAndNextFileNames(lastFileName).map {
          case (nextFetch, newFiles) =>
            lastCheckedState.setLastCheckedAt(SDate.now())
            Option((nextFetch, newFiles), (lastFileName, lastFiles))
        }.recover {
          case t =>
            log.error(s"Failed to get next files after $lastFileName : ${t.getMessage}")
            Option((lastFileName, lastFiles), (lastFileName, lastFiles))
        }
      }
      .throttle(1, throttle)
      .map(_._2)
      .mapConcat(identity)
      .flatMapConcat { zipFileName =>
        fileProcessor.process(zipFileName)
          .map {
            case Some((total, successful)) =>
              log.info(s"$successful / $total manifests successfully processed from $zipFileName")
              metricsCollector.counter("api-dq-manifests-processed", successful)
            case None =>
              log.info(s"$zipFileName could not be processed")
              metricsCollector.counter("api-dq-zip-failure", 1)
          }
          .recover {
            case t => log.error(s"Failed to process files after $lastFileName: ${t.getMessage}")
          }
          .map(_ => zipFileName)
      }
      .wireTap(s =>
        if (s.nonEmpty)
          metricsCollector.counter("api-dq-zip-processed", 1)
        else
          metricsCollector.counter("api-dq-zip-processed", 0)
      )


  private def markerAndNextFileNames(lastFile: String): Future[(String, List[String])] =
    fileNamesProvider.nextFiles(lastFile)
      .map { fileNames =>
        val files = if (lastFile.nonEmpty) fileNames.filterNot(_.contains(lastFile)) else fileNames
        val nextFetch = files.sorted.reverse.headOption.getOrElse(lastFile)
        (nextFetch, files)
      }
}

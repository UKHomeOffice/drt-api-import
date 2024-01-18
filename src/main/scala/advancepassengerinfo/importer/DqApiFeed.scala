package advancepassengerinfo.importer

import advancepassengerinfo.health.ProcessState
import advancepassengerinfo.importer.processor.DqFileProcessor
import advancepassengerinfo.importer.provider.FileNames
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import metrics.MetricsCollectorLike

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait DqApiFeed {
  def processFilesAfter(lastFileName: String): Source[String, NotUsed]
}

case class DqApiFeedImpl(fileNamesProvider: FileNames,
                         fileProcessor: DqFileProcessor,
                         throttle: FiniteDuration,
                         metricsCollector: MetricsCollectorLike,
                         processState: ProcessState)
                        (implicit ec: ExecutionContext) extends DqApiFeed {
  private val log = Logger(getClass)

  override def processFilesAfter(lastFileName: String): Source[String, NotUsed] =
    Source
      .unfoldAsync((lastFileName, List[String]())) { case (lastFileName, lastFiles) =>
        markerAndNextFileNames(lastFileName).map {
          case (nextFetch, newFiles) => Option((nextFetch, newFiles), (lastFileName, lastFiles))
        }
      }
      .throttle(1, throttle)
      .map(_._2)
      .mapConcat(identity)
      .flatMapConcat { zipFileName =>
        fileProcessor.process(zipFileName)
          .map {
            case Some((total, successful)) =>
              processState.update()
              log.info(s"$successful / $total manifests successfully processed from $zipFileName")
              metricsCollector.counter("api-dq-manifests-processed", successful)
            case None =>
              processState.update()
              log.info(s"$zipFileName could not be processed")
              metricsCollector.counter("api-dq-zip-failure", 1)
          }
          .map(_ => zipFileName)
      }
      .wireTap(_ => metricsCollector.counter("api-dq-zip-processed", 1))

  private def markerAndNextFileNames(lastFile: String): Future[(String, List[String])] =
    fileNamesProvider.nextFiles(lastFile)
      .map { fileNames =>
        val files = if (lastFile.nonEmpty) fileNames.filterNot(_.contains(lastFile)) else fileNames
        val nextFetch = files.sorted.reverse.headOption.getOrElse(lastFile)
        (nextFetch, files)
      }
}

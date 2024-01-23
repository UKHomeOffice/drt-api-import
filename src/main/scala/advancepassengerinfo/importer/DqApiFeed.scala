package advancepassengerinfo.importer

import advancepassengerinfo.importer.processor.DqFileProcessor
import advancepassengerinfo.importer.provider.FileNames
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import drtlib.SDate
import drtlib.SDate.yyyyMMdd
import metrics.MetricsCollectorLike

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

trait DqApiFeed {
  def processFilesAfter(lastFileName: String): Source[String, NotUsed]
}

case class DqApiFeedImpl(fileNamesProvider: FileNames,
                         fileProcessor: DqFileProcessor,
                         throttle: FiniteDuration,
                         metricsCollector: MetricsCollectorLike)
                        (implicit ec: ExecutionContext) extends DqApiFeed {
  private val log = Logger(getClass)

  override def processFilesAfter(lastFileName: String): Source[String, NotUsed] =
    Source
      .unfoldAsync((lastFileName, List[String]())) { case (lastFileName, lastFiles) =>
        val fallbackFileName = "drt_dq_" + yyyyMMdd(SDate.now().addDays(-1)) + "_000000_0000.zip"
        markerAndNextFileNames(lastFileName, fallbackFileName).map {
          case (nextFetch, newFiles) =>
            Option((nextFetch, newFiles), (lastFileName, lastFiles))
        }.recover {
          case t =>
            log.error(s"Failed to get next files after $lastFileName : ${t.getMessage}")
            Option((lastFileName, lastFiles), (lastFileName, lastFiles))
        }
      }.throttle(1, throttle)
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
          .map(_ => zipFileName)
      }.recoverWithRetries(3, { case t =>
      log.error(s"Failed to process files after $lastFileName: ${t.getMessage}")
      Source.empty
    }).wireTap(s =>
      if (s.nonEmpty)
        metricsCollector.counter("api-dq-zip-processed", 1)
      else
        metricsCollector.counter("api-dq-zip-processed", 0)
    )


  private def markerAndNextFileNames(lastFile: String, fallbackFileName: String): Future[(String, List[String])] =
    fileNamesProvider.nextFiles(lastFile, fallbackFileName)
      .map { fileNames =>
        val files = if (lastFile.nonEmpty) fileNames.filterNot(_.contains(lastFile)) else fileNames
        val nextFetch = files.sorted.reverse.headOption.getOrElse(lastFile)
        (nextFetch, files)
      }
}

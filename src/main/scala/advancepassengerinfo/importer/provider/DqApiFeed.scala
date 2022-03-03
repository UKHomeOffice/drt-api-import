package advancepassengerinfo.importer.provider

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait DqApiFeed {
  def processFilesAfter(lastFileName: String): Source[String, NotUsed]
}

case class DqApiFeedImpl(fileNameProvider: DqFileNameProvider,
                         fileProcessor: DqFileProcessor,
                         throttle: FiniteDuration)
                        (implicit ec: ExecutionContext) extends DqApiFeed {
  private val log = Logger(getClass)

  def processFilesAfter(lastFileName: String): Source[String, NotUsed] =
    Source
      .unfoldAsync((lastFileName, List[String]())) { case (lastFileName, lastFiles) =>
        fileNameProvider.markerAndNextFileNames(lastFileName).map {
          case (nextFetch, newFiles) =>
            log.info(s"from $lastFileName, new files: $newFiles")
            log.info(s"next fetch: $nextFetch")
            Option((nextFetch, newFiles), (lastFileName, lastFiles))
        }
      }
      .log("filenames")
      .throttle(1, throttle)
      .map(_._2)
      .mapConcat(identity)
      .flatMapConcat { zipFileName =>
        fileProcessor.process(zipFileName)
          .map {
            case Some((total, successful)) =>
              log.info(s"$successful / $total manifests successfully processed from $zipFileName")
            case None =>
              log.info(s"$zipFileName could not be processed")
          }
          .map(_ => zipFileName)
      }
}

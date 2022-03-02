package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.persistence.Persistence
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait DqFileImporter {
  def processFilesAfter(lastFileName: String): Source[String, NotUsed]
}

case class DqFileImporterImpl(fileNameProvider: DqFileNameProvider,
                              fileProcessor: DqFileProcessor,
                              persistence: Persistence,
                              throttle: FiniteDuration)
                             (implicit ec: ExecutionContext) extends DqFileImporter {
  private val log = Logger(getClass)

  def processFilesAfter(lastFileName: String): Source[String, NotUsed] =
    Source
      .unfoldAsync((lastFileName, List[String]())) { case (lastFileName, lastFiles) =>
        fileNameProvider.markerAndNextFileNames(lastFileName).map {
          case (nextFetch, newFiles) => Option((nextFetch, newFiles), (lastFileName, lastFiles))
        }
      }
      .throttle(1, throttle)
      .map(_._2)
      .mapConcat(identity)
      .flatMapConcat { zipFileName =>
        fileProcessor
          .process(zipFileName)
          .mapAsync(1) { maybeCount =>
            maybeCount match {
              case Some(manifestCount) =>
                log.info(s"$zipFileName processed. $manifestCount manifests found")
              case None =>
                log.error(s"$zipFileName could not be processed")
            }
            persistence.persistZipFile(zipFileName, maybeCount.isDefined).map(_ => zipFileName)
          }
      }
}

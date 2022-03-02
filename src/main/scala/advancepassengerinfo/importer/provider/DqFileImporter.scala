package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.persistence.Persistence
import akka.NotUsed
import akka.stream.scaladsl.Source

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
          .map(_.isDefined)
          .mapAsync(1) { success =>
            persistence.persistZipFile(zipFileName, success).map(_ => zipFileName)
          }
      }
}

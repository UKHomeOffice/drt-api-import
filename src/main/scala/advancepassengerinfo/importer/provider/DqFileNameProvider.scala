package advancepassengerinfo.importer.provider

import scala.concurrent.{ExecutionContext, Future}

case class DqFileNameProvider(s3FileNamesProvider: S3FileNamesProvider)
                             (implicit ec: ExecutionContext) {
  val markerAndNextFileNames: String => Future[(String, List[String])] =
    (lastFile: String) => s3FileNamesProvider.nextFiles(lastFile)
      .map { fileNames =>
        val files = if (lastFile.nonEmpty) fileNames.filterNot(_.contains(lastFile)) else fileNames
        val nextFetch = files.sorted.reverse.headOption.getOrElse(lastFile)
        (nextFetch, files)
      }
}

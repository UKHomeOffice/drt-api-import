package apiimport.provider

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import apiimport.manifests.VoyageManifestParser.VoyageManifest
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}


case class LocalApiProvider(pathToFiles: String)(implicit actorSystem: ActorSystem, materializer: Materializer, executionContext: ExecutionContext) extends ApiProviderLike {
  val log = Logger(getClass)

  def manifestsStream(latestFile: String): Source[(String, List[Either[(String, String), (String, VoyageManifest)]]), NotUsed] = {
    log.info(s"Requesting DQ zip files > ${latestFile.take(20)}")
    filesAsSource
      .filter(f => filterNewer(latestFile)(f.getName))
      .mapAsync(2) { file =>
        log.info(s"Processing ${file.getName}")
        val zipByteStream = new FileInputStream(file)
        val zipInputStream = new ZipInputStream(zipByteStream)

        Future(fileNameAndContentFromZip(file.getName, zipInputStream))
          .map { case (zipFileName, jsons) => (zipFileName, jsonsOrManifests(jsons)) }
      }
  }

  def filesAsSource: Source[File, NotUsed] = Source(getListOfFiles(pathToFiles).sortBy(_.getName))

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)

    if (d.exists && d.isDirectory)
      d.listFiles.filter(_.isFile).toList
    else
      List[File]()
  }
}

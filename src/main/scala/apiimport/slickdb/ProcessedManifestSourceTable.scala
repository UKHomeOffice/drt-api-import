package apiimport.slickdb

import java.sql.Timestamp

import apiimport.manifests.VoyageManifestParser
import apiimport.manifests.VoyageManifestParser.VoyageManifest
import com.typesafe.scalalogging.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try


case class ProcessedManifestSourceTable(tables: Tables) {
  val log = Logger(getClass)

  import tables.profile.api._
  import tables.{ProcessedManifestSource, ProcessedManifestSourceRow}
}

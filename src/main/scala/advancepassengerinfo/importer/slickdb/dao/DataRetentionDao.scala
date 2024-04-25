package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonTable, ProcessedZipTable, VoyageManifestPassengerInfoTable}
import drtlib.SDate
import org.slf4j.LoggerFactory
import slick.lifted.TableQuery

import scala.concurrent.{ExecutionContext, Future}

case class DataRetentionDao(db: Db)
                           (implicit ec: ExecutionContext) {
  private val log = LoggerFactory.getLogger(getClass)

  private val zipTable = TableQuery[ProcessedZipTable]
  private val jsonTable = TableQuery[ProcessedJsonTable]
  private val jsonWithZip = jsonTable join zipTable on (_.zip_file_name === _.zip_file_name)
  private val manifestTable = TableQuery[VoyageManifestPassengerInfoTable]

  def deleteForDate(date: SDate, maxBatchSize: Int): Future[(Int, Int, Int)] = {
    val isoDate = date.toIsoDate

    val query = jsonWithZip
      .filter(_._2.created_on === isoDate)
      .map {
        case (json, _) => json.json_file_name
      }
      .sortBy(_.asc)
      .take(maxBatchSize)
      .result

    db.run(query)
      .flatMap { jsonFileNames =>
        val deleteManifests = manifestTable.filter(_.json_file.inSet(jsonFileNames)).delete
        val deleteJsons = jsonTable.filter(_.json_file_name.inSet(jsonFileNames)).delete

        if (jsonFileNames.size < maxBatchSize) {
          log.info(s"Deleting final batch of data (${jsonFileNames.size} jsons) and manifests & zip for $isoDate")
          val deleteZips = zipTable.filter(_.created_on === isoDate).delete
          db.run(DBIO.sequence(Seq(deleteManifests, deleteJsons, deleteZips)).transactionally)
            .map(counts => (counts.head, counts(1), counts(2)))
        } else {
          log.info(s"Deleting $maxBatchSize jsons and manifests for $isoDate")
          db.run(DBIO.sequence(Seq(deleteManifests, deleteJsons)).transactionally)
            .map(counts => (counts.head, counts(1), 0))
        }
      }
  }
}

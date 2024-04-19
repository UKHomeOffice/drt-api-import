package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonTable, ProcessedZipTable, VoyageManifestPassengerInfoTable}
import slick.lifted.TableQuery

import scala.concurrent.{ExecutionContext, Future}

case class DataRetentionDao(db: Db)
                           (implicit ec: ExecutionContext) {
  private val zipTable = TableQuery[ProcessedZipTable]
  private val jsonTable = TableQuery[ProcessedJsonTable]
  private val jsonWithZip = jsonTable join zipTable on (_.zip_file_name === _.zip_file_name)
  private val manifestTable = TableQuery[VoyageManifestPassengerInfoTable]

  def deleteForDate(date: String): Future[(Int, Int, Int)] = {
    val query = jsonWithZip
      .filter(_._2.created_on === date)
      .map {
        case (json, _) => json.json_file_name
      }
      .result

    db.run(query)
      .flatMap { jsonFileNames =>
        val deleteManifests = manifestTable.filter(_.json_file.inSet(jsonFileNames)).delete
        val deleteJsons = jsonTable.filter(_.json_file_name.inSet(jsonFileNames)).delete
        val deleteZips = zipTable.filter(_.created_on === date).delete
        for {
          deletedManifests <- db.run(deleteManifests)
          deletedJsons <- db.run(deleteJsons)
          deletedZips <- db.run(deleteZips)
        } yield (deletedZips, deletedJsons, deletedManifests)
      }
  }
}

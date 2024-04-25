package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.generator.ManifestGenerator
import advancepassengerinfo.importer.InMemoryDatabase
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.ProcessedJsonGenerator
import advancepassengerinfo.importer.slickdb.serialisation.VoyageManifestSerialisation.voyageManifestRows
import advancepassengerinfo.importer.slickdb.tables._
import advancepassengerinfo.manifests.VoyageManifest
import drtlib.SDate
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import slick.lifted.TableQuery

import java.sql.Timestamp
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class DataRetentionDaoTest extends AnyWordSpec with Matchers with BeforeAndAfter {
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  private val zipDao = ProcessedZipDaoImpl(InMemoryDatabase)
  private val jsonDao = ProcessedJsonDaoImpl(InMemoryDatabase)
  private val manifestDao = VoyageManifestPassengerInfoDaoImpl(InMemoryDatabase)
  private val retentionDao = DataRetentionDao(InMemoryDatabase)

  private val zipTable = TableQuery[ProcessedZipTable]
  private val jsonTable = TableQuery[ProcessedJsonTable]
  private val manifestsTable = TableQuery[VoyageManifestPassengerInfoTable]

  private val zip0101 = ProcessedZipRow("drt_dq_210101_144059_6361.zip", success = true, new Timestamp(0), Option("2021-01-01"))
  private val zip0102 = ProcessedZipRow("drt_dq_210102_144059_6362.zip", success = true, new Timestamp(0), Option("2021-01-02"))
  private val zip0103 = ProcessedZipRow("drt_dq_210103_144059_6363.zip", success = true, new Timestamp(0), Option("2021-01-03"))
  private val json0101 = ProcessedJsonGenerator.unpopulated("drt_dq_210101_144059_6361.zip", "drt_dq_210101_144059_6361.json")
  private val json0102 = ProcessedJsonGenerator.unpopulated("drt_dq_210102_144059_6362.zip", "drt_dq_210102_144059_6362.json")
  private val json0103 = ProcessedJsonGenerator.unpopulated("drt_dq_210103_144059_6363.zip", "drt_dq_210103_144059_6363.json")
  private val manifests0101 = voyageManifestRows(ManifestGenerator.manifest("2021-01-01", "10:00"), 1, 2, "drt_dq_210101_144059_6361.json")
  private val manifests0102 = voyageManifestRows(ManifestGenerator.manifest("2021-01-02", "10:00"), 2, 2, "drt_dq_210102_144059_6362.json")
  private val manifests0103 = voyageManifestRows(ManifestGenerator.manifest("2021-01-03", "10:00"), 3, 2, "drt_dq_210103_144059_6363.json")

  before {
    InMemoryDatabase.dropAndCreateTables
    val inserts = zipDao.insert(zip0101)
      .flatMap(_ => zipDao.insert(zip0102))
      .flatMap(_ => zipDao.insert(zip0103))
      .flatMap(_ => jsonDao.insert(json0101))
      .flatMap(_ => jsonDao.insert(json0102))
      .flatMap(_ => jsonDao.insert(json0103))
      .flatMap(_ => manifestDao.insert(manifests0101))
      .flatMap(_ => manifestDao.insert(manifests0102))
      .flatMap(_ => manifestDao.insert(manifests0103))
      .flatMap(_ => jsonDao.populateManifestColumnsForDate("2021-01-01"))
      .flatMap(_ => jsonDao.populateManifestColumnsForDate("2021-01-02"))
      .flatMap(_ => jsonDao.populateManifestColumnsForDate("2021-01-03"))

    Await.result(inserts, 1.second)
  }

  "deleteForDate" should {
    "delete all records for the given date" in {
      val jsonDeletionBatchSize = 500
      val (zips1, jsons1, manifests1) = zipsJsonsAndManifests

      zips1 should have size 3
      jsons1 should have size 3
      manifests1 should have size 3

      val dateToDelete = SDate("2021-01-02")
      Await.ready(retentionDao.deleteForDate(dateToDelete, jsonDeletionBatchSize), 1.second)

      val (zips2, jsons2, manifests2) = zipsJsonsAndManifests

      zips2.map(_.zip_file_name) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.zip", "drt_dq_210103_144059_6363.zip")
      jsons2.map(_.json_file_name) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.json", "drt_dq_210103_144059_6363.json")
      manifests2.map(_.json_file) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.json", "drt_dq_210103_144059_6363.json")
    }

    "delete up to batch size records for the given date, followed by the remaining records if there are fewer than batch size left" in {
      val batchSize = 2
      val json0102a = ProcessedJsonGenerator.unpopulated("drt_dq_210102_144059_6362.zip", "drt_dq_210102_144059_0001.json")
      val manifests0102a = voyageManifestRows(ManifestGenerator.manifest("2021-01-02", "10:00"), 2, 2, "drt_dq_210102_144059_0001.json")
      val json0102b = ProcessedJsonGenerator.unpopulated("drt_dq_210102_144059_6362.zip", "drt_dq_210102_144059_0002.json")
      val manifests0102b = voyageManifestRows(ManifestGenerator.manifest("2021-01-02", "10:00"), 2, 2, "drt_dq_210102_144059_0002.json")

      Await.ready(
        jsonDao
          .insert(json0102a)
          .flatMap(_ => jsonDao.insert(json0102b))
          .flatMap(_ => manifestDao.insert(manifests0102a))
          .flatMap(_ => manifestDao.insert(manifests0102b))
          .flatMap(_ => jsonDao.populateManifestColumnsForDate("2021-01-02")),
        1.second
      )

      val (zips1, jsons1, manifests1) = zipsJsonsAndManifests

      zips1 should have size 3
      jsons1 should have size 5
      manifests1 should have size 5

      val dateToDelete = SDate("2021-01-02")
      Await.result(retentionDao.deleteForDate(dateToDelete, batchSize), 1.second) shouldBe(batchSize, batchSize, 0)

      val (zips2, jsons2, manifests2) = zipsJsonsAndManifests

      zips2.map(_.zip_file_name) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.zip", "drt_dq_210102_144059_6362.zip", "drt_dq_210103_144059_6363.zip")
      jsons2.map(_.json_file_name) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.json", "drt_dq_210102_144059_6362.json", "drt_dq_210103_144059_6363.json")
      manifests2.map(_.json_file) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.json", "drt_dq_210102_144059_6362.json", "drt_dq_210103_144059_6363.json")

      Await.result(retentionDao.deleteForDate(dateToDelete, batchSize), 1.second) shouldBe(1, 1, 1)

      val (zips3, jsons3, manifests3) = zipsJsonsAndManifests

      zips3.map(_.zip_file_name) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.zip", "drt_dq_210103_144059_6363.zip")
      jsons3.map(_.json_file_name) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.json", "drt_dq_210103_144059_6363.json")
      manifests3.map(_.json_file) should contain theSameElementsAs Seq("drt_dq_210101_144059_6361.json", "drt_dq_210103_144059_6363.json")
    }
  }

  private def zipsJsonsAndManifests: (Seq[ProcessedZipRow], Seq[ProcessedJsonRow], Seq[VoyageManifestPassengerInfoRow]) = {
    val result = InMemoryDatabase.run(zipTable.result)
      .flatMap(zipRows => InMemoryDatabase.run(jsonTable.result).map(jsonRows => (zipRows, jsonRows)))
      .flatMap { case (zipRows, jsonRows) =>
        InMemoryDatabase.run(manifestsTable.result).map(manifestRows => (zipRows, jsonRows, manifestRows))
      }

    val (zips, jsons, manifests) = Await.result(result, 1.second)
    (zips, jsons, manifests)
  }
}

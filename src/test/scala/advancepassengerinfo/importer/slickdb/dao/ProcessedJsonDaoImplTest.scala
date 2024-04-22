package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.generator.ManifestGenerator
import advancepassengerinfo.importer.InMemoryDatabase
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.ProcessedJsonGenerator
import advancepassengerinfo.importer.slickdb.serialisation.VoyageManifestSerialisation.voyageManifestRows
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonTable, ProcessedZipRow}
import drtlib.SDate
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import slick.lifted.TableQuery

import java.sql.Timestamp
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class ProcessedJsonDaoImplTest extends AnyWordSpec with Matchers with BeforeAndAfter {
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  private val dao = ProcessedJsonDaoImpl(InMemoryDatabase)
  private val zipDao = ProcessedZipDaoImpl(InMemoryDatabase)
  private val manifestDao = VoyageManifestPassengerInfoDaoImpl(InMemoryDatabase)
  private val table = TableQuery[ProcessedJsonTable]

  before {
    InMemoryDatabase.dropAndCreateTables
  }

  "insert" should {
    "persist a json file record" in {
      val row = ProcessedJsonGenerator.populated("test.zip", "test.json")
      val result = dao.insert(row)
        .flatMap(_ => InMemoryDatabase.run(table.result))
      Await.result(result, 1.second) shouldBe Seq(row)
    }
  }

  "jsonHasBeenProcessed" should {
    "return true if the json file has been processed" in {
      val row = ProcessedJsonGenerator.populated("test.zip", "test.json")
      val result = dao.insert(row)
        .flatMap(_ => dao.jsonHasBeenProcessed("test.zip", "test.json"))
      Await.result(result, 1.second) shouldBe true
    }
    "return false if the json file has not been processed" in {
      val result = dao.jsonHasBeenProcessed("test.zip", "test.json")
      Await.result(result, 1.second) shouldBe false
    }
  }

  "earliestUnpopulatedDate" should {
    "return the earliest unpopulated date" in {
      val zipRow1 = ProcessedZipRow("test1.zip", success = true, new Timestamp(0), Option("2021-01-01"))
      val zipRow2 = ProcessedZipRow("test2.zip", success = true, new Timestamp(0), Option("2021-01-02"))
      val row1 = ProcessedJsonGenerator.unpopulated("test1.zip", "testa.json")
      val row2 = ProcessedJsonGenerator.populated("test2.zip", "testb.json")
      val result = zipDao.insert(zipRow1)
        .flatMap(_ => zipDao.insert(zipRow2))
        .flatMap(_ => dao.insert(row1))
        .flatMap(_ => dao.insert(row2))
        .flatMap(_ => dao.earliestUnpopulatedDate)
      Await.result(result, 1.second) shouldBe Some("2021-01-01")
    }
  }

  "updateManifestColumnsForDate" should {
    "update the manifest columns for a date" in {
      val zipRow = ProcessedZipRow("test.zip", success = true, new Timestamp(0), Option("2021-01-01"))
      val jsonRow = ProcessedJsonGenerator.unpopulated("test.zip", "test.json")
      val manifest = ManifestGenerator.manifest("2021-01-01", "01:30")
      val result = zipDao.insert(zipRow)
        .flatMap(_ => dao.insert(jsonRow))
        .flatMap(_ => manifestDao.insert(voyageManifestRows(manifest, 1, 2, "test.json")))
        .flatMap(_ => dao.updateManifestColumnsForDate("2021-01-01"))
        .flatMap(_ => InMemoryDatabase.run(table.result.head))

      val updatedJsonRow = Await.result(result, 1.second)

      updatedJsonRow shouldBe jsonRow.copy(
        arrival_port_code = Option(manifest.ArrivalPortCode),
        departure_port_code = Option(manifest.DeparturePortCode),
        voyage_number = Option(manifest.VoyageNumber.toInt),
        carrier_code = Option(manifest.CarrierCode),
        scheduled = manifest.scheduleArrivalDateTime.map(s => new Timestamp(s.millisSinceEpoch)),
        event_code = Option(manifest.EventCode),
        non_interactive_total_count = Option(0),
        non_interactive_trans_count = Option(0),
        interactive_total_count = Option(1),
        interactive_trans_count = Option(0),
      )
    }
  }

  "delete" should {
    "delete a row from the table" in {
      val row1 = ProcessedJsonGenerator.populated("test1.zip", "testa.json")
      val row2 = ProcessedJsonGenerator.populated("test2.zip", "testb.json")
      val rows1 = dao.insert(row1)
        .flatMap(_ => dao.insert(row2))
        .flatMap(_ => InMemoryDatabase.run(table.result))
      Await.result(rows1, 1.second) should ===(Seq(row1, row2))

      val rows2 = dao.delete("testa.json").flatMap(_ => InMemoryDatabase.run(table.result))
      Await.result(rows2, 1.second) should ===(Seq(row2))
    }
  }
}

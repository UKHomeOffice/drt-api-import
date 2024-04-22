package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.generator.ManifestGenerator
import advancepassengerinfo.importer.InMemoryDatabase
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.serialisation.VoyageManifestSerialisation.voyageManifestRows
import advancepassengerinfo.importer.slickdb.tables.{VoyageManifestPassengerInfoRow, VoyageManifestPassengerInfoTable}
import drtlib.SDate
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import slick.lifted.TableQuery

import java.sql.Timestamp
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.DurationInt

class VoyageManifestPassengerInfoDaoImplTest extends AnyWordSpec with Matchers with BeforeAndAfter {
  implicit val ec: ExecutionContext = scala.concurrent.ExecutionContext.Implicits.global

  private val dao = VoyageManifestPassengerInfoDaoImpl(InMemoryDatabase)
  private val table = TableQuery[VoyageManifestPassengerInfoTable]

  before {
    InMemoryDatabase.dropAndCreateTables
  }

  "persistManifest" should {
    "persist a manifest" in {
      val manifest = ManifestGenerator.manifest("2024-04-22", "10:30")
      val result = dao.insert(voyageManifestRows(manifest, 1, 2, "test.json"))
        .flatMap(_ => InMemoryDatabase.run(table.result))
      val scheduled = new Timestamp(SDate("2024-04-22T10:30:00.0").millisSinceEpoch)

      Await.result(result, 1.second) shouldBe Vector(VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 1000, "BA",
        scheduled, 1, 2, "P", "GBR", "Y", 22, "LHR", "N", "GBR", "GBR", "123", in_transit = false, "test.json"))
    }
  }

  "dayOfWeekAndWeekOfYear" should {
    "return the day of the week and week of the year" in {
      val date = new Timestamp(SDate("2024-04-22T10:30:00.0").millisSinceEpoch)
      val result = dao.dayOfWeekAndWeekOfYear(date)
      Await.result(result, 1.second) shouldBe (1, 17)
    }
  }

  "delete" should {
    "delete a row from the table" in {
      val manifest = ManifestGenerator.manifest("2024-04-22", "10:30")
      val man1Rows = voyageManifestRows(manifest, 1, 2, "test1.json")
      val man2Rows = voyageManifestRows(manifest, 3, 4, "test2.json")
      val result = dao.insert(man1Rows)
        .flatMap(_ => dao.insert(man2Rows))
        .flatMap(_ => InMemoryDatabase.run(table.result))
      Await.result(result, 1.second) should have size 2

      val rows = dao.delete("test1.json").flatMap(_ => InMemoryDatabase.run(table.result))
      Await.result(rows, 1.second) shouldBe man2Rows
    }
  }
}

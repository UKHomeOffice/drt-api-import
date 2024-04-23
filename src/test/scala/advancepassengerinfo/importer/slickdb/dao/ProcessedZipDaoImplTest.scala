package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.importer.InMemoryDatabase
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.tables.{ProcessedZipRow, ProcessedZipTable}
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import slick.lifted.TableQuery

import java.sql.Timestamp
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class ProcessedZipDaoImplTest extends AnyWordSpec with Matchers with BeforeAndAfter {
  private val dao = ProcessedZipDaoImpl(InMemoryDatabase)
  private val table = TableQuery[ProcessedZipTable]
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  before {
    InMemoryDatabase.dropAndCreateTables
  }

  "insert" should {
    "insert a row into the table" in {
      val row = ProcessedZipRow("test.zip", success = true, new Timestamp(0), Option("2021-01-01"))
      val result = dao.insert(row)
        .flatMap(_ => InMemoryDatabase.run(table.result.headOption))
      Await.result(result, 1.second) shouldBe Some(row)
    }
  }

  "lastPersistedFileName" should {
    "return the last persisted file name" in {
      val row1 = ProcessedZipRow("test1.zip", success = true, new Timestamp(0), Option("2021-01-01"))
      val row2 = ProcessedZipRow("test2.zip", success = true, new Timestamp(0), Option("2021-01-01"))
      val result = dao.insert(row1)
        .flatMap(_ => dao.insert(row2))
        .flatMap(_ => dao.lastPersistedFileName)
      Await.result(result, 1.second) shouldBe Some("test2.zip")
    }
  }

  "delete" should {
    "delete a row from the table" in {
      val row1 = ProcessedZipRow("test1.zip", success = true, new Timestamp(0), Option("2021-01-01"))
      val row2 = ProcessedZipRow("test2.zip", success = true, new Timestamp(0), Option("2021-01-01"))
      val rows1 = dao.insert(row1)
        .flatMap(_ => dao.insert(row2))
        .flatMap(_ => InMemoryDatabase.run(table.result))
      Await.result(rows1, 1.second) should ===(Seq(row1, row2))

      val rows2 = dao.delete("test1.zip").flatMap(_ => InMemoryDatabase.run(table.result))
      Await.result(rows2, 1.second) should ===(Seq(row2))
    }
  }

  "oldestDate" should {
    "return the oldest date" in {
      val row1 = ProcessedZipRow("test1.zip", success = true, new Timestamp(0), Option("2021-01-01"))
      val row2 = ProcessedZipRow("test2.zip", success = true, new Timestamp(0), Option("2021-01-02"))
      val result = dao.insert(row1)
        .flatMap(_ => dao.insert(row2))
        .flatMap(_ => dao.oldestDate)
      Await.result(result, 1.second) shouldBe Some("2021-01-01")
    }
  }
}

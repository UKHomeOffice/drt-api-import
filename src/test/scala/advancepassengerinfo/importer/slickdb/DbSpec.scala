package advancepassengerinfo.importer.slickdb

import java.sql.Timestamp

import advancepassengerinfo.importer.InMemoryDatabase
import drtlib.SDate
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps


class DbSpec extends FlatSpec with Matchers with Builder {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val vmTable: VoyageManifestPassengerInfoTable = VoyageManifestPassengerInfoTable(InMemoryDatabase.tables)

  "A request for day of the week" should "give me 7 (for Saturday) when given a date falling on a Saturday (according to h2's definitions)" in {
    val date = new Timestamp(SDate("2019-04-20T12:00:00Z").millisSinceEpoch)
    val sql = vmTable.dayOfWeekAndWeekOfYear(date)
    val result = Await.result(InMemoryDatabase.con.run(sql), 1 second).collect {
      case (dow, _) => dow
    }

    result should be(Some(7))
  }

  "A request for day of the week" should "give me 1 (for Sunday) when given a date falling on a Sunday (according to h2's definitions)" in {
    val date = new Timestamp(SDate("2019-04-21T12:00:00Z").millisSinceEpoch)
    val sql = vmTable.dayOfWeekAndWeekOfYear(date)
    val result = Await.result(InMemoryDatabase.con.run(sql), 1 second).collect {
      case (dow, _) => dow
    }

    result should be(Some(1))
  }

  "A request for week of the year" should "give me 1 when given a date falling in the first week of the year (according to hs's definitions)" in {
    val date = new Timestamp(SDate("2019-01-01T12:00:00Z").millisSinceEpoch)
    val sql = vmTable.dayOfWeekAndWeekOfYear(date)
    val result = Await.result(InMemoryDatabase.con.run(sql), 1 second).collect {
      case (_, woy) => woy
    }

    result should be(Some(1))
  }

  "A request for week of the year" should "give me 52 when given a date falling in the last week of the year (according to h2's definitions)" in {
    val date = new Timestamp(SDate("2018-12-30T12:00:00Z").millisSinceEpoch)
    val sql = vmTable.dayOfWeekAndWeekOfYear(date)
    val result = Await.result(InMemoryDatabase.con.run(sql), 1 second).collect {
      case (_, woy) => woy
    }

    result should be(Some(52))
  }
}

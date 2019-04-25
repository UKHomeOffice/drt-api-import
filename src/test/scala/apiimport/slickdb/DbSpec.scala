package apiimport.slickdb

import java.sql.Timestamp

import apiimport.manifests.VoyageManifestParser.{PassengerInfoJson, VoyageManifest}
import drtlib.SDate
import org.scalatest._
import apiimport.slickdb.{Tables, ProcessedManifestSourceTable}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}

object H2Tables extends {
  val profile = slick.jdbc.H2Profile
} with Tables

class DbSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  import H2Tables.profile.api._

  val vmTable: ProcessedManifestSourceTable = ProcessedManifestSourceTable(H2Tables)
  val db = Database.forConfig("tsql.db")

  override def beforeAll(): Unit = {
    Await.ready(db.run(H2Tables.schema.create), 1 second)
  }

  override def afterAll(): Unit = {
    db.close()
  }

  "A request for day of the week" should "give me 7 (for Saturday) when given a date falling on a Saturday (according to h2's definitions)" in {
    val date = new Timestamp(SDate("2019-04-20T12:00:00Z").millisSinceEpoch)
    val sql = vmTable.dayOfWeekAndWeekOfYear(date)
    val result = Await.result(db.run(sql), 1 second).collect {
      case (dow, _) => dow
    }

    result should be(Some(7))
  }

  "A request for day of the week" should "give me 1 (for Sunday) when given a date falling on a Sunday (according to h2's definitions)" in {
    val date = new Timestamp(SDate("2019-04-21T12:00:00Z").millisSinceEpoch)
    val sql = vmTable.dayOfWeekAndWeekOfYear(date)
    val result = Await.result(db.run(sql), 1 second).collect {
      case (dow, _) => dow
    }

    result should be(Some(1))
  }

  "A request for week of the year" should "give me 1 when given a date falling in the first week of the year (according to hs's definitions)" in {
    val date = new Timestamp(SDate("2019-01-01T12:00:00Z").millisSinceEpoch)
    val sql = vmTable.dayOfWeekAndWeekOfYear(date)
    val result = Await.result(db.run(sql), 1 second).collect {
      case (_, woy) => woy
    }

    result should be(Some(1))
  }

  "A request for week of the year" should "give me 52 when given a date falling in the last week of the year (according to h2's definitions)" in {
    val date = new Timestamp(SDate("2018-12-30T12:00:00Z").millisSinceEpoch)
    val sql = vmTable.dayOfWeekAndWeekOfYear(date)
    val result = Await.result(db.run(sql), 1 second).collect {
      case (_, woy) => woy
    }

    result should be(Some(52))
  }

  "A request to insert a VoyageManifest" should "result in a row being inserted for each passenger" in {
    val vm = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "12:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("10"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("12345")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("25"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("22331"))
    ))

    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))
    val (dayOfWeek, weekOfYear) = Await.result(db.run(vmTable.dayOfWeekAndWeekOfYear(schTs)), 1 second).getOrElse((-1, -1))

    val jsonFile = "test.json"
    Await.ready(db.run(vmTable.rowsToInsert(vm, dayOfWeek, weekOfYear, jsonFile)), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result

    val result = Await.result(db.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "GBR", "T", 10, "LHR", "N", "GBR", "GBR", "12345", in_transit = false, jsonFile),
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "I", "GBR", "F", 25, "LHR", "N", "GBR", "GBR", "22331", in_transit = false, jsonFile))

    result should be(expected)
  }
}

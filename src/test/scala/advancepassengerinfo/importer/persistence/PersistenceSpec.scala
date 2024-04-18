package advancepassengerinfo.importer.persistence

import advancepassengerinfo.importer.InMemoryDatabase.H2Tables.profile.api._
import advancepassengerinfo.importer.slickdb.dao.{ProcessedJsonDaoImpl, ProcessedZipDaoImpl, VoyageManifestPassengerInfoDaoImpl}
import advancepassengerinfo.importer.slickdb.tables._
import advancepassengerinfo.importer.slickdb.tables
import advancepassengerinfo.importer.{InMemoryDatabase, PostgresDateHelpers}
import advancepassengerinfo.manifests.{PassengerInfo, VoyageManifest}
import akka.actor.ActorSystem
import akka.stream.Materializer
import drtlib.SDate
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import slick.lifted.TableQuery

import java.sql.Timestamp
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps


class PersistenceSpec extends AnyWordSpec with Matchers with BeforeAndAfter {
  implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
  implicit val materializer: Materializer = Materializer.createMaterializer(actorSystem)
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  before {
    InMemoryDatabase.dropAndCreateTables
  }

  private val manifestsDao = VoyageManifestPassengerInfoDaoImpl(InMemoryDatabase)
  private val zipDao = ProcessedZipDaoImpl(InMemoryDatabase)
  private val jsonDao = ProcessedJsonDaoImpl(InMemoryDatabase)

  private val manifestsTable = TableQuery[VoyageManifestPassengerInfoTable]
  private val jsonTable = TableQuery[ProcessedJsonTable]
  private val zipTable = TableQuery[ProcessedZipTable]

  private val schDateStr = "2019-01-01"
  private val schDate = SDate(schDateStr)
  private val schDayOfTheWeek: Int = PostgresDateHelpers.dayOfTheWeek(schDate)

  private val validManifest = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", schDateStr, "06:00", List(
    PassengerInfo(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
    PassengerInfo(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002"))
  ))
  private val invalidManifest = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "yyyy-mm-dd", "06:00", List(
    PassengerInfo(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
  ))
  private val processedAt = SDate("2024-04-15T12:56").millisSinceEpoch

  "A request to insert a VoyageManifest" should {
    "result in a row being inserted for each passenger" in {
      val vm = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", schDateStr, "12:00", List(
        PassengerInfo(Some("P"), "GBR", "T", Some("10"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("12345")),
        PassengerInfo(Some("I"), "GBR", "F", Some("25"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("22331"))
      ))

      val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))
      val (dayOfWeek, weekOfYear) = Await.result(manifestsDao.dayOfWeekAndWeekOfYear(schTs), 1 second)

      val jsonFile = "test.json"
      Await.ready(manifestsDao.insertManifest(vm, dayOfWeek, weekOfYear, jsonFile), 1 second)

      val paxEntries = manifestsTable.result

      val result = Await.result(InMemoryDatabase.con.run(paxEntries), 1 second)

      val expected = Vector(
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "P", "GBR", "T", 10, "LHR", "N", "GBR", "GBR", "12345", in_transit = false, jsonFile),
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "I", "GBR", "F", 25, "LHR", "N", "GBR", "GBR", "22331", in_transit = false, jsonFile))

      result should be(expected)
    }
  }

  "A request to persist a VoyageManifest from a zip file" should {
    "result in entries in the VoyageManifestPassenger & ProcessedJson & ProcessedZip tables" in {
      val schTs = new Timestamp(validManifest.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

      val jsonFile = "someJson"
      val zipFile = "drt_dq_240415_134518_2296.zip"

      val processedAt = SDate("2024-04-15T12:56:38.204").millisSinceEpoch

      Await.ready(manifestsDao.persistManifest(jsonFile, validManifest, validManifest.scheduleArrivalDateTime.get), 1 second)
      Await.ready(jsonDao.persistJsonFile(zipFile, jsonFile, successful = true, dateIsSuspicious = false, Option(validManifest), processedAt), 1 second)
      val maybeCreatedOn = ProcessedZipRow.extractCreatedOn(zipFile)
      val zipRow = ProcessedZipRow(zipFile, success = true, new Timestamp(processedAt), maybeCreatedOn)
      Await.ready(zipDao.insert(zipRow), 1 second)

      val paxEntries = manifestsTable.result
      val paxRows = Await.result(InMemoryDatabase.con.run(paxEntries), 1 second)
      val jsonEntries = jsonTable.result
      val jsonRows = Await.result(InMemoryDatabase.con.run(jsonEntries), 1 second)
      val zipEntries = zipTable.result
      val zipRows = Await.result(InMemoryDatabase.con.run(zipEntries), 1 second)
      val lastPersistedFileName = Await.result(zipDao.lastPersistedFileName, 1 second)

      val expectedPaxRows = Vector(
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile))
      val expectedJsonRows = List(ProcessedJsonRow(zipFile, jsonFile, suspicious_date = false, success = true, new Timestamp(processedAt), Some("LHR"), Some("JFK"), Some(123), Some(new Timestamp(SDate("2019-01-01T06:00:00.0").millisSinceEpoch)), Some("DC"), Some(0), Some(0), Some(2), Some(0)))
      val expectedZipRows = List(ProcessedZipRow(zipFile, success = true, new Timestamp(processedAt), Option("2024-04-15")))

      paxRows should ===(expectedPaxRows)
      jsonRows should ===(expectedJsonRows)
      zipRows should ===(expectedZipRows)
      lastPersistedFileName should ===(Some(zipFile))
    }
  }

  "Persisting 2 manifests for the same flight in the same stream" should {
    "result in all entries being persisted" in {
      val vm = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", schDateStr, "06:00", List(
        PassengerInfo(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
        PassengerInfo(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002"))
      ))
      val vm2 = vm.copy(PassengerList = List(
        PassengerInfo(Some("P"), "FRA", "T", Some("99"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00003"))
      ))
      val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

      val jsonFile = "someJson"

      Await.ready(manifestsDao.persistManifest(jsonFile, vm, vm.scheduleArrivalDateTime.get), 1 second)
      Await.ready(manifestsDao.persistManifest(jsonFile, vm2, vm2.scheduleArrivalDateTime.get), 1 second)

      val paxEntries = manifestsTable.result
      val result = Await.result(InMemoryDatabase.con.run(paxEntries), 1 second).toSet

      val expected = Set(
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile),
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "P", "FRA", "T", 99, "LHR", "N", "GBR", "GBR", "00003", in_transit = false, jsonFile),
      )

      result should be(expected)
    }
  }

  "Persisting 2 manifests for the same flight with different event codes" should {
    "result in all entries being persisted" in {
      val vmDc = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", schDateStr, "06:00", List(
        PassengerInfo(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
        PassengerInfo(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002"))
      ))
      val vmCi = vmDc.copy(EventCode = "CI", PassengerList = List(
        PassengerInfo(Some("P"), "FRA", "T", Some("99"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00003"))
      ))
      val schTs = new Timestamp(vmDc.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

      val jsonFile = "someJson"

      Await.ready(manifestsDao.persistManifest(jsonFile, vmDc, vmDc.scheduleArrivalDateTime.get), 1 second)
      Await.ready(manifestsDao.persistManifest(jsonFile, vmCi, vmCi.scheduleArrivalDateTime.get), 1 second)

      val paxEntries = manifestsTable.result
      val result = Await.result(InMemoryDatabase.con.run(paxEntries), 1 second)

      val expected = Vector(
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile),
        VoyageManifestPassengerInfoRow("CI", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "P", "FRA", "T", 99, "LHR", "N", "GBR", "GBR", "00003", in_transit = false, jsonFile))

      result should be(expected)
    }
  }

  "Persisting a manifest containing both iAPI and non-iAPI pax" should {
    "result only in only the iAPI pax entries being persisted" in {
      val vmDc = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", schDateStr, "06:00", List(
        PassengerInfo(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
        PassengerInfo(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002")),
        PassengerInfo(Some("P"), "FRA", "T", Some("80"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("")),
        PassengerInfo(Some("P"), "FRA", "T", Some("81"), Some("LHR"), "N", Some("GBR"), Some("GBR"), None)
      ))
      val schTs = new Timestamp(vmDc.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

      val jsonFile = "someJson"

      Await.ready(manifestsDao.persistManifest(jsonFile, vmDc, vmDc.scheduleArrivalDateTime.get), 1 second)

      val paxEntries = manifestsTable.result
      val result = Await.result(InMemoryDatabase.con.run(paxEntries), 1 second)

      val expected = Vector(
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
        VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, schDayOfTheWeek, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile))

      result should be(expected)
    }
  }

  "Persisting a failed zip file" should {
    "result in an entry in the processed_zip table" in {
      val zipFile = "drt_dq_240415_134518_2296.zip"
      val processedAt = SDate("2024-04-15T12:56")

      val maybeCreatedOn = ProcessedZipRow.extractCreatedOn(zipFile)
      val zipRow = ProcessedZipRow(zipFile, success = false, new Timestamp(processedAt.millisSinceEpoch), maybeCreatedOn)
      Await.ready(zipDao.insert(zipRow), 1 second)

      val zipEntries = zipTable.result
      val zipRowsAsTuple = Await.result(InMemoryDatabase.con.run(zipEntries), 1 second)

      val expected = List(tables.ProcessedZipRow(zipFile, success = false, new Timestamp(processedAt.millisSinceEpoch), Option("2024-04-15")))

      zipRowsAsTuple should ===(expected)
    }
  }

  "Persisting a failed json file" should {
    "result in an entry in the processed_json table" in {
      val zipFile = "drt_dq_240415_134518_2296.zip"
      val jsonFile = "someJson"

      Await.ready(jsonDao.persistJsonFile(zipFile, jsonFile, successful = false, dateIsSuspicious = false, Option(validManifest), processedAt), 1 second)

      val jsonEntries = jsonTable.result
      val jsonRowsAsTuple = Await.result(InMemoryDatabase.con.run(jsonEntries), 1 second)

      val scheduled = new Timestamp(SDate("2019-01-01T06:00:00.0").millisSinceEpoch)

      val expected = List(ProcessedJsonRow(
        zip_file_name = "drt_dq_240415_134518_2296.zip",
        json_file_name = "someJson",
        suspicious_date = false,
        success = false,
        processed_at = new Timestamp(processedAt),
        arrival_port_code = Some("LHR"),
        departure_port_code = Some("JFK"),
        voyage_number = Some(123),
        scheduled = Some(scheduled),
        event_code = Some("DC"),
        non_interactive_total_count = Some(0),
        non_interactive_trans_count = Some(0),
        interactive_total_count = Some(2),
        interactive_trans_count = Some(0)))

      jsonRowsAsTuple should be(expected)
    }
  }

//  "Persisting a manifest with an nonsense schedule date" should {
//    "result in no row being inserted and a Future(None) return value" in {
//      val result = Await.ready(manifestsDao.persistManifest("someJson", invalidManifest, invalidManifest.scheduleArrivalDateTime.get), 1 second)
//      val rows = Await.result(InMemoryDatabase.con.run(manifestsTable.result), 1 second)
//      result.value.get.get should be(None)
//      rows should be(empty)
//    }
//  }

//  "When a persisting a manifest throws an exception then the exception is caught and logged" in {
//    dropTables()
//    val result = Await.ready(manifestsDao.persistManifest("someJson", validManifest, validManifest.scheduleArrivalDateTime.get), 1 second)
//    result.value.get.get should be(None)
//  }

  private def dropTables(): Unit =
    Await.result(
      InMemoryDatabase.con.run(DBIO.seq(
        TableQuery[ProcessedZipTable].schema.dropIfExists,
        TableQuery[ProcessedJsonTable].schema.dropIfExists,
        TableQuery[VoyageManifestPassengerInfoTable].schema.dropIfExists,
      )
      ), 1.second
    )
}

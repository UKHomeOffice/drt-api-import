package advancepassengerinfo.importer.persistence

import java.sql.Timestamp

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import advancepassengerinfo.importer.H2Db
import advancepassengerinfo.importer.H2Db.H2Tables
import advancepassengerinfo.importer.manifests.VoyageManifestParser.{PassengerInfoJson, VoyageManifest}
import advancepassengerinfo.importer.slickdb.{Builder, VoyageManifestPassengerInfoTable}
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


class ManifestPersistenceSpec extends FlatSpec with Matchers with Builder {
  implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val vmTable: VoyageManifestPassengerInfoTable = VoyageManifestPassengerInfoTable(H2Tables)

  import advancepassengerinfo.importer.H2Db.tables.profile.api._


  val persistor = ManifestPersistor(H2Db)

  "A request to insert a VoyageManifest" should "result in a row being inserted for each passenger" in {
    val vm = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "12:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("10"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("12345")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("25"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("22331"))
    ))

    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))
    val (dayOfWeek, weekOfYear) = Await.result(H2Db.con.run(vmTable.dayOfWeekAndWeekOfYear(schTs)), 1 second).getOrElse((-1, -1))

    val jsonFile = "test.json"
    Await.ready(H2Db.con.run(vmTable.rowsToInsert(vm, dayOfWeek, weekOfYear, jsonFile)), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result

    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "GBR", "T", 10, "LHR", "N", "GBR", "GBR", "12345", in_transit = false, jsonFile),
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "I", "GBR", "F", 25, "LHR", "N", "GBR", "GBR", "22331", in_transit = false, jsonFile))

    result should be(expected)
  }

  "A request to persist a VoyageManifest from a zip file" should "result in entries in the VoyageManifestPassenger & ProcessedJson & ProcessedZip tables" in {
    val vm = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "06:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002"))
    ))
    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    val jsonFile = "someJson"
    val zipFile = "someZip"
    val manifestSource = Source(List((zipFile, Success(List((jsonFile, Success(vm)))))))

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val paxRows = Await.result(H2Db.con.run(paxEntries), 1 second)
    val jsonEntries = H2Tables.ProcessedJson.result
    val jsonRowsAsTuple = Await.result(H2Db.con.run(jsonEntries), 1 second).map(r => (r.zip_file_name, r.json_file_name, r.suspicious_date, r.success))
    val zipEntries = H2Tables.ProcessedZip.result
    val zipRowsAsTuple = Await.result(H2Db.con.run(zipEntries), 1 second).map(r => (r.zip_file_name, r.success))

    val expectedPaxRows = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile))
    val expectedJsonRowsAsTuple = List((zipFile, jsonFile, false, true))
    val expectedZipRowsAsTuple = List((zipFile, true))

    paxRows should be(expectedPaxRows)
    jsonRowsAsTuple should be(expectedJsonRowsAsTuple)
    zipRowsAsTuple should be(expectedZipRowsAsTuple)
  }

  "Persisting 2 manifests for the same flight in the same stream" should "result only in entries from the second manifest" in {
    val vm = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "06:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002"))
    ))
    val vm2 = vm.copy(PassengerList = List(
      PassengerInfoJson(Some("P"), "FRA", "T", Some("99"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00003"))
    ))
    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    val jsonFile = "someJson"
    val zipFile = "someZip"
    val manifestSource = Source(List((zipFile, Success(List((jsonFile, Success(vm)), (jsonFile, Success(vm2)))))))

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "FRA", "T", 99, "LHR", "N", "GBR", "GBR", "00003", in_transit = false, jsonFile))

    result should be(expected)
  }

  "Persisting 2 manifests for the same flight in consecutive streams" should "result only in entries from the second manifest" in {
    val vm = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "06:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002"))
    ))
    val vm2 = vm.copy(PassengerList = List(
      PassengerInfoJson(Some("P"), "FRA", "T", Some("99"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00003"))
    ))
    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    val jsonFile = "someJson"
    val zipFile = "someZip"
    val manifestSource = Source(List((zipFile, Success(List((jsonFile, Success(vm)))))))
    val manifestSource2 = Source(List((zipFile, Success(List((jsonFile, Success(vm2)))))))

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)
    Await.ready(persistor.addPersistence(manifestSource2).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "FRA", "T", 99, "LHR", "N", "GBR", "GBR", "00003", in_transit = false, jsonFile))

    result should be(expected)
  }

  "Persisting 2 manifests for the same flight with different event codes" should "result only in all entries being persisted" in {
    val vmDc = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "06:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002"))
    ))
    val vmCi = vmDc.copy(EventCode = "CI", PassengerList = List(
      PassengerInfoJson(Some("P"), "FRA", "T", Some("99"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00003"))
    ))
    val schTs = new Timestamp(vmDc.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    val jsonFile = "someJson"
    val zipFile = "someZip"
    val manifestSource = Source(List((zipFile, Success(List((jsonFile, Success(vmDc)))))))
    val manifestSource2 = Source(List((zipFile, Success(List((jsonFile, Success(vmCi)))))))

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)
    Await.ready(persistor.addPersistence(manifestSource2).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile),
      H2Tables.VoyageManifestPassengerInfoRow("CI", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "FRA", "T", 99, "LHR", "N", "GBR", "GBR", "00003", in_transit = false, jsonFile))

    result should be(expected)
  }

  "Persisting a manifest containing both iAPI and non-iAPI pax" should "result only in only the iAPI pax entries being persisted" in {
    val vmDc = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "06:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002")),
      PassengerInfoJson(Some("P"), "FRA", "T", Some("80"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("")),
      PassengerInfoJson(Some("P"), "FRA", "T", Some("81"), Some("LHR"), "N", Some("GBR"), Some("GBR"), None)
    ))
    val schTs = new Timestamp(vmDc.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    val jsonFile = "someJson"
    val zipFile = "someZip"
    val manifestSource = Source(List((zipFile, Success(List((jsonFile, Success(vmDc)))))))

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile))

    result should be(expected)
  }

  "Persisting a failed zip file" should "result in an entry in the processed_zip table" in {
    val persistor = ManifestPersistor(H2Db)

    val zipFile = "someZip"
    val failure: Try[List[(String, Try[VoyageManifest])]] = Failure(new Exception("yeah"))
    val manifestSource = Source(List((zipFile, failure)))

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)

    val zipEntries = H2Tables.ProcessedZip.result
    val zipRowsAsTuple = Await.result(H2Db.con.run(zipEntries), 1 second).map(r => (r.zip_file_name, r.success))

    val expected = List((zipFile, false))

    zipRowsAsTuple should be (expected)
  }

  "Persisting a failed json file" should "result in an entry in the processed_json table" in {
    val persistor = ManifestPersistor(H2Db)

    val zipFile = "someJson"
    val jsonFile = "someJson"
    val failure: Try[VoyageManifest] = Failure(new Exception("yeah"))
    val manifestSource = Source(List((zipFile, Success(List((jsonFile, failure))))))

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)

    val jsonEntries = H2Tables.ProcessedJson.result
    val jsonRowsAsTuple = Await.result(H2Db.con.run(jsonEntries), 1 second).map(r => (r.zip_file_name, r.json_file_name, r.suspicious_date, r.success))

    val expected = List((zipFile, jsonFile, false, false))

    jsonRowsAsTuple should be (expected)
  }
}

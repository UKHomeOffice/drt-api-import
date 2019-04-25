package apiimport.persistence

import java.sql.Timestamp

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import apiimport.H2Db
import apiimport.H2Db.H2Tables
import apiimport.manifests.VoyageManifestParser.{PassengerInfoJson, VoyageManifest}
import apiimport.slickdb.{Builder, VoyageManifestPassengerInfoTable}
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps


class ManifestPersistenceSpec extends FlatSpec with Matchers with Builder {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val vmTable: VoyageManifestPassengerInfoTable = VoyageManifestPassengerInfoTable(H2Tables)

  import apiimport.H2Db.tables.profile.api._

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

  "A request to persist a VoyageManifest from a zip file" should "result in entries in the VoyageManifestPassenger & ProcessedManifestSource tables" in {
    val persistor = ManifestPersistor(H2Db)

    val vm = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "06:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002"))
    ))
    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    val jsonFile = "someJson"
    val zipFile = "someZip"
    val manifestSource = Source(List(
      (zipFile, List((jsonFile, vm)), List())
    ))

    implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile))

    result should be(expected)
  }

  "Persisting 2 manifests for the same flight in the same stream" should "result only in entries from the second manifest" in {
    val persistor = ManifestPersistor(H2Db)

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
    val manifestSource = Source(List((zipFile, List((jsonFile, vm), (jsonFile, vm2)), List())))

    implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "FRA", "T", 99, "LHR", "N", "GBR", "GBR", "00003", in_transit = false, jsonFile))

    result should be(expected)
  }

  "Persisting 2 manifests for the same flight in consecutive streams" should "result only in entries from the second manifest" in {
    val persistor = ManifestPersistor(H2Db)

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
    val manifestSource = Source(List((zipFile, List((jsonFile, vm)), List())))
    val manifestSource2 = Source(List((zipFile, List((jsonFile, vm2)), List())))

    implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)
    Await.ready(persistor.addPersistence(manifestSource2).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "FRA", "T", 99, "LHR", "N", "GBR", "GBR", "00003", in_transit = false, jsonFile))

    result should be(expected)
  }

  "Persisting 2 manifests for the same flight with different event codes" should "result only in all entries being persisted" in {
    val persistor = ManifestPersistor(H2Db)

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
    val manifestSource = Source(List((zipFile, List((jsonFile, vmDc)), List())))
    val manifestSource2 = Source(List((zipFile, List((jsonFile, vmCi)), List())))

    implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

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
    val persistor = ManifestPersistor(H2Db)

    val vmDc = VoyageManifest("DC", "LHR", "JFK", "0123", "BA", "2019-01-01", "06:00", List(
      PassengerInfoJson(Some("P"), "GBR", "T", Some("1"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00001")),
      PassengerInfoJson(Some("I"), "GBR", "F", Some("2"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("00002")),
      PassengerInfoJson(Some("P"), "FRA", "T", Some("80"), Some("LHR"), "N", Some("GBR"), Some("GBR"), Some("")),
      PassengerInfoJson(Some("P"), "FRA", "T", Some("81"), Some("LHR"), "N", Some("GBR"), Some("GBR"), None)
    ))
    val schTs = new Timestamp(vmDc.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    val jsonFile = "someJson"
    val zipFile = "someZip"
    val manifestSource = Source(List((zipFile, List((jsonFile, vmDc)), List())))

    implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    Await.ready(persistor.addPersistence(manifestSource).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val result = Await.result(H2Db.con.run(paxEntries), 1 second)

    val expected = Vector(
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "P", "GBR", "T", 1, "LHR", "N", "GBR", "GBR", "00001", in_transit = false, jsonFile),
      H2Tables.VoyageManifestPassengerInfoRow("DC", "LHR", "JFK", 123, "BA", schTs, 3, 1, "I", "GBR", "F", 2, "LHR", "N", "GBR", "GBR", "00002", in_transit = false, jsonFile))

    result should be(expected)
  }
}

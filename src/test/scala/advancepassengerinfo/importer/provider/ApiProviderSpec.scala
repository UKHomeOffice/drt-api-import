package advancepassengerinfo.importer.provider

import advancepassengerinfo.importer.{InMemoryDatabase, PostgresDateHelpers}
import advancepassengerinfo.importer.persistence.ManifestPersistor
import advancepassengerinfo.importer.slickdb.{Builder, VoyageManifestPassengerInfoTable}
import advancepassengerinfo.manifests.{PassengerInfo, VoyageManifest}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import drtlib.SDate
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.sql.Timestamp
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps
import scala.util.{Success, Try}


class ApiProviderSpec extends AnyWordSpec with Matchers with Builder {
  implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
  implicit val materializer: Materializer = Materializer.createMaterializer(actorSystem)
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val vmTable: VoyageManifestPassengerInfoTable = VoyageManifestPassengerInfoTable(InMemoryDatabase.tables)
  val provider: TestApiProvider = TestApiProvider()
  val persistor: ManifestPersistor = ManifestPersistor(InMemoryDatabase, 6)

  "A provider with a valid zip containing one json manifest" should {
    "return a success case representation of that processed zip" in {
      val manifestsStream: Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed] = provider.manifestsStream("")

      val stuff: immutable.Seq[(String, Try[List[(String, Try[VoyageManifest])]])] = Await.result(manifestsStream.runWith(Sink.seq), 1 second)

      val expected = Vector(
        ("manifest.zip", Success(List(("manifest.json", Success(VoyageManifest("DC", "STN", "BRE", "3631", "FR", "2016-03-02", "07:30:00", List(
          PassengerInfo(Some("P"), "MAR", "", Some("21"), Some("STN"), "N", Some("GBR"), Some("MAR"), Some("000")),
          PassengerInfo(Some("G"), "", "", Some("43"), Some("STN"), "N", Some("GBR"), Some(""), Some(""))))))))))

      stuff should be(expected)
    }
  }

  "A provider with a single valid manifest with both an iAPI and a non-iAPI passenger" should {
    "result in only the iAPI passenger record being recorded when passed through a persistor" in {
      import advancepassengerinfo.importer.InMemoryDatabase.tables.profile.api._

      val manifestsStream: Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed] = provider.manifestsStream("")

      Await.ready(persistor.addPersistenceToStream(manifestsStream).runWith(Sink.seq), 1 second)

      val paxEntries = InMemoryDatabase.tables.VoyageManifestPassengerInfo.result
      val paxRows = Await.result(InMemoryDatabase.con.run(paxEntries), 1 second)

      val schDate = SDate("2016-03-02T07:30:00.0")
      val schTs = new Timestamp(schDate.millisSinceEpoch)
      val dayOfWeek = PostgresDateHelpers.dayOfTheWeek(schDate)
      val expected = Vector(InMemoryDatabase.tables.VoyageManifestPassengerInfoRow("DC", "STN", "BRE", 3631, "FR", schTs, dayOfWeek, 9, "P", "MAR", "", 21, "STN", "N", "GBR", "MAR", "000", in_transit = false, "manifest.json"))

      paxRows should be(expected)
    }
  }
}

package apiimport.provider

import java.sql.Timestamp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import apiimport.H2Db
import apiimport.H2Db.H2Tables
import apiimport.manifests.VoyageManifestParser.{PassengerInfoJson, VoyageManifest}
import apiimport.persistence.ManifestPersistor
import apiimport.slickdb.{Builder, VoyageManifestPassengerInfoTable}
import drtlib.SDate
import org.scalatest._

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


class ApiProviderSpec extends FlatSpec with Matchers with Builder {
  implicit val actorSystem: ActorSystem = ActorSystem("api-data-import")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  val vmTable: VoyageManifestPassengerInfoTable = VoyageManifestPassengerInfoTable(H2Tables)
  val provider = TestApiProvider()
  val persistor = ManifestPersistor(H2Db)

  "A provider with a valid zip containing one json manifest" should "return a success case representation of that processed zip" in {
    val manifestsStream: Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed] = provider.manifestsStream("")

    val stuff: immutable.Seq[(String, Try[List[(String, Try[VoyageManifest])]])] = Await.result(manifestsStream.runWith(Sink.seq), 1 second)

    val expected = Vector(
      ("manifest.zip", Success(List(("manifest.json", Success(VoyageManifest("DC", "STN", "BRE", "3631", "FR", "2016-03-02", "07:30:00", List(
        PassengerInfoJson(Some("P"), "MAR", "", Some("21"), Some("STN"), "N", Some("GBR"), Some("MAR"), Some("000")),
        PassengerInfoJson(Some("G"), "", "", Some("43"), Some("STN"), "N", Some("GBR"), Some(""), Some(""))))))))))

    stuff should be(expected)
  }

  "A provider with a single valid manifest with both an iAPI and a non-iAPI passenger" should "result in only the iAPI passenger record being recorded when passed through a persistor" in {
    import apiimport.H2Db.tables.profile.api._

    val manifestsStream: Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed] = provider.manifestsStream("")

    Await.ready(persistor.addPersistence(manifestsStream).runWith(Sink.seq), 1 second)

    val paxEntries = H2Tables.VoyageManifestPassengerInfo.result
    val paxRows = Await.result(H2Db.con.run(paxEntries), 1 second)

    val schTs = new Timestamp(SDate("2016-03-02T07:30:00.0").millisSinceEpoch)
    val expected = Vector(H2Tables.VoyageManifestPassengerInfoRow("DC", "STN", "BRE", 3631, "FR", schTs, 4, 9, "P", "MAR", "", 21, "STN", "N", "GBR", "MAR", "000", in_transit = false, "manifest.json"))

    paxRows should be(expected)
  }
}

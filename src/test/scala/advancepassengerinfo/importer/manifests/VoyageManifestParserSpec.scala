package advancepassengerinfo.importer.manifests

import advancepassengerinfo.importer.manifests.VoyageManifestParser.{PassengerInfoJson, VoyageManifest}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class VoyageManifestParserSpec extends FlatSpec with Matchers {
  val validJsonManifest =
    """
      |{
      |  "EventCode": "DC",
      |  "DeparturePortCode": "BRE",
      |  "VoyageNumberTrailingLetter": "",
      |  "ArrivalPortCode": "STN",
      |  "DeparturePortCountryCode": "DEU",
      |  "VoyageNumber": "3631",
      |  "VoyageKey": "517c62be54d6822e33424d0fd7057449",
      |  "ScheduledDateOfDeparture": "2016-03-02",
      |  "ScheduledDateOfArrival": "2016-03-02",
      |  "CarrierType": "AIR",
      |  "CarrierCode": "FR",
      |  "ScheduledTimeOfDeparture": "06:00:00",
      |  "ScheduledTimeOfArrival": "07:30:00",
      |  "FileId": "drt_160302_060000_FR3631_DC_4089",
      |  "PassengerList": [
      |    {
      |      "DocumentIssuingCountryCode": "MAR",
      |      "PersonType": "P",
      |      "DocumentLevel": "Primary",
      |      "Age": "21",
      |      "DisembarkationPortCode": "STN",
      |      "InTransitFlag": "N",
      |      "DisembarkationPortCountryCode": "GBR",
      |      "NationalityCountryEEAFlag": "",
      |      "DocumentType": "P",
      |      "PoavKey": "000d6ab0f4929d8a92a99b83b0c35cfc",
      |      "NationalityCountryCode": "MAR"
      |    },
      |    {
      |      "DocumentIssuingCountryCode": "",
      |      "PersonType": "P",
      |      "DocumentLevel": "Primary",
      |      "Age": "43",
      |      "DisembarkationPortCode": "STN",
      |      "InTransitFlag": "N",
      |      "DisembarkationPortCountryCode": "GBR",
      |      "NationalityCountryEEAFlag": "",
      |      "DocumentType": "G",
      |      "PoavKey": "0081267f2eb0cb7c2ab650dbc0b825b3",
      |      "NationalityCountryCode": ""
      |    }
      |  ]
      |}
    """.stripMargin

  val invalidJsonManifest = """{}"""

  "A valid json manifest" should "give a valid VoyageManifest" in {
    val vmTry = VoyageManifestParser.parseVoyagePassengerInfo(validJsonManifest)

    val expected = Success(VoyageManifest("DC","STN","BRE","3631","FR","2016-03-02","07:30:00",List(
      PassengerInfoJson(Some("P"),"MAR","",Some("21"),Some("STN"),"N",Some("GBR"),Some("MAR"),None),
      PassengerInfoJson(Some("G"),"","",Some("43"),Some("STN"),"N",Some("GBR"),Some(""),None))))

    vmTry should be (expected)
  }

  "An invalid json manifest" should "give a Failure" in {
    val isFailure = VoyageManifestParser.parseVoyagePassengerInfo(invalidJsonManifest) match {
      case Failure(_) => true
      case _ => false
    }

    val expected = true

    isFailure should be (expected)
  }
}

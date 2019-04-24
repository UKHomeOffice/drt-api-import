package apiimport.manifests

import com.typesafe.scalalogging.Logger
import drtlib.SDate
import org.joda.time.DateTime
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.util.Try

object VoyageManifestParser {
  val log = Logger(getClass)

  def parseVoyagePassengerInfo(content: String): Try[VoyageManifest] = {
    import FlightPassengerInfoProtocol._
    import spray.json._
    Try(content.parseJson.convertTo[VoyageManifest])
  }

  case class PassengerInfoJson(DocumentType: Option[String],
                               DocumentIssuingCountryCode: String,
                               EEAFlag: String,
                               Age: Option[String] = None,
                               DisembarkationPortCode: Option[String],
                               InTransitFlag: String = "N",
                               DisembarkationPortCountryCode: Option[String] = None,
                               NationalityCountryCode: Option[String] = None,
                               PassengerIdentifier: Option[String]
                              )

  case class VoyageManifest(EventCode: String,
                            ArrivalPortCode: String,
                            DeparturePortCode: String,
                            VoyageNumber: String,
                            CarrierCode: String,
                            ScheduledDateOfArrival: String,
                            ScheduledTimeOfArrival: String,
                            PassengerList: List[PassengerInfoJson]) {
    def scheduleArrivalDateTime: Option[SDate] = Try(DateTime.parse(scheduleDateTimeString)).toOption.map(SDate(_))

    def scheduleDateTimeString: String = s"${ScheduledDateOfArrival}T${ScheduledTimeOfArrival}Z"
  }

  object FlightPassengerInfoProtocol extends DefaultJsonProtocol {
    implicit val passengerInfoConverter: RootJsonFormat[PassengerInfoJson] = jsonFormat(PassengerInfoJson,
      "DocumentType",
      "DocumentIssuingCountryCode",
      "NationalityCountryEEAFlag",
      "Age",
      "DisembarkationPortCode",
      "InTransitFlag",
      "DisembarkationPortCountryCode",
      "NationalityCountryCode",
      "PassengerIdentifier"
    )
    implicit val passengerInfoResponseConverter: RootJsonFormat[VoyageManifest] = jsonFormat8(VoyageManifest)
  }

}

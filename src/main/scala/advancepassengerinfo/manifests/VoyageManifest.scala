package advancepassengerinfo.manifests

import drtlib.SDate
import org.joda.time.DateTime

import scala.util.Try


case class VoyageManifest(EventCode: String,
                          ArrivalPortCode: String,
                          DeparturePortCode: String,
                          VoyageNumber: String,
                          CarrierCode: String,
                          ScheduledDateOfArrival: String,
                          ScheduledTimeOfArrival: String,
                          PassengerList: List[PassengerInfo]) {
  def scheduleArrivalDateTime: Option[SDate] = Try(DateTime.parse(scheduleDateTimeString)).toOption.map(SDate(_))

  def scheduleDateTimeString: String = s"${ScheduledDateOfArrival}T${ScheduledTimeOfArrival}Z"

  def interactivePassengers: List[PassengerInfo] = PassengerList.filter {
    case PassengerInfo(_, _, _, _, _, _, _, _, Some(id)) => id != ""
    case _ => false
  }

  def nonInteractivePassengers: List[PassengerInfo] = PassengerList.filterNot {
    case PassengerInfo(_, _, _, _, _, _, _, _, Some(id)) => id != ""
    case _ => false
  }

  def hasInteractivePassengers: Boolean = PassengerList.exists {
    case PassengerInfo(_, _, _, _, _, _, _, _, Some(id)) => id != ""
    case _ => false
  }

  def bestPassengers: List[PassengerInfo] = if (hasInteractivePassengers) interactivePassengers else nonInteractivePassengers
}

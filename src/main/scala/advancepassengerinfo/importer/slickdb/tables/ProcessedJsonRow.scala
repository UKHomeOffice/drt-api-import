package advancepassengerinfo.importer.slickdb.tables

import advancepassengerinfo.manifests.VoyageManifest

import java.sql.Timestamp
import scala.util.Try

case class ProcessedJsonRow(zip_file_name: String,
                            json_file_name: String,
                            suspicious_date: Boolean,
                            success: Boolean,
                            processed_at: Timestamp,
                            arrival_port_code: Option[String],
                            departure_port_code: Option[String],
                            voyage_number: Option[Int],
                            carrier_code: Option[String],
                            scheduled: Option[Timestamp],
                            event_code: Option[String],
                            non_interactive_total_count: Option[Int],
                            non_interactive_trans_count: Option[Int],
                            interactive_total_count: Option[Int],
                            interactive_trans_count: Option[Int],
                           )

object ProcessedJsonRow {
  def fromManifest(zipFileName: String,
                   jsonFileName: String,
                   successful: Boolean,
                   dateIsSuspicious: Boolean,
                   maybeManifest: Option[VoyageManifest],
                   processedAt: Long,
                  ): ProcessedJsonRow =
    ProcessedJsonRow(
      zip_file_name = zipFileName,
      json_file_name = jsonFileName,
      suspicious_date = dateIsSuspicious,
      success = successful,
      processed_at = new Timestamp(processedAt),
      arrival_port_code = maybeManifest.map(_.ArrivalPortCode),
      departure_port_code = maybeManifest.map(_.DeparturePortCode),
      voyage_number = Try(maybeManifest.map(_.VoyageNumber.toInt)).toOption.flatten,
      carrier_code = maybeManifest.map(_.CarrierCode),
      scheduled = maybeManifest.flatMap(m => m.scheduleArrivalDateTime.map(s => new Timestamp(s.millisSinceEpoch))),
      event_code = maybeManifest.map(_.EventCode),
      non_interactive_total_count = maybeManifest.map(_.PassengerList.count(!_.PassengerIdentifier.exists(_ != ""))),
      non_interactive_trans_count = maybeManifest.map(_.PassengerList.count(p => !p.PassengerIdentifier.exists(_ != "") && p.InTransitFlag.contains("Y"))),
      interactive_total_count = maybeManifest.map(_.PassengerList.count(_.PassengerIdentifier.exists(_ != ""))),
      interactive_trans_count = maybeManifest.map(_.PassengerList.count(p => p.PassengerIdentifier.exists(_ != "") && p.InTransitFlag.contains("Y"))),
    )
}

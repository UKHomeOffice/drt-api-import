package advancepassengerinfo.importer.slickdb.tables

import java.sql.Timestamp

case class ProcessedJsonRow(zip_file_name: String,
                            json_file_name: String,
                            suspicious_date: Boolean,
                            success: Boolean,
                            processed_at: Timestamp,
                            arrival_port_code: Option[String],
                            departure_port_code: Option[String],
                            voyage_number: Option[Int],
                            scheduled: Option[Timestamp],
                            event_code: Option[String],
                            non_interactive_total_count: Option[Int],
                            non_interactive_trans_count: Option[Int],
                            interactive_total_count: Option[Int],
                            interactive_trans_count: Option[Int],
                           )

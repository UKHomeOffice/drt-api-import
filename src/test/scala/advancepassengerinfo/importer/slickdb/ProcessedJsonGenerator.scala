package advancepassengerinfo.importer.slickdb

import advancepassengerinfo.importer.slickdb.tables.ProcessedJsonRow

object ProcessedJsonGenerator {
  def populated(zipFileName: String, jsonFileName: String): ProcessedJsonRow = ProcessedJsonRow(
    zip_file_name = zipFileName,
    json_file_name = jsonFileName,
    suspicious_date = false,
    success = true,
    processed_at = new java.sql.Timestamp(0),
    arrival_port_code = Option("LHR"),
    departure_port_code = Option("JFK"),
    voyage_number = Option(1),
    carrier_code = Option("BA"),
    scheduled = Option(new java.sql.Timestamp(0)),
    event_code = Option("DC"),
    non_interactive_total_count = Option(1),
    non_interactive_trans_count = Option(1),
    interactive_total_count = Option(1),
    interactive_trans_count = Option(1),
  )

  def unpopulated(zipFileName: String, jsonFileName: String): ProcessedJsonRow = ProcessedJsonRow(
    zip_file_name = zipFileName,
    json_file_name = jsonFileName,
    suspicious_date = false,
    success = true,
    processed_at = new java.sql.Timestamp(0),
    arrival_port_code = None,
    departure_port_code = None,
    voyage_number = None,
    carrier_code = None,
    scheduled = None,
    event_code = None,
    non_interactive_total_count = None,
    non_interactive_trans_count = None,
    interactive_total_count = None,
    interactive_trans_count = None,
  )
}

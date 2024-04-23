package advancepassengerinfo.importer.slickdb.tables

case class VoyageManifestPassengerInfoRow(event_code: String,
                                          arrival_port_code: String,
                                          departure_port_code: String,
                                          voyage_number: Int,
                                          carrier_code: String,
                                          scheduled_date: java.sql.Timestamp,
                                          day_of_week: Int,
                                          week_of_year: Int,
                                          document_type: String,
                                          document_issuing_country_code: String,
                                          eea_flag: String,
                                          age: Int,
                                          disembarkation_port_code: String,
                                          in_transit_flag: String,
                                          disembarkation_port_country_code: String,
                                          nationality_country_code: String,
                                          passenger_identifier: String,
                                          in_transit: Boolean,
                                          json_file: String)
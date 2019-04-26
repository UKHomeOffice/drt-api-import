package apiimport.slickdb

import java.sql.Timestamp

import slick.jdbc.PostgresProfile

/** Slick data model trait for extension, choice of backend or usage in the cake pattern. (Make sure to initialize this late.) */
trait Tables {
  val profile: slick.jdbc.JdbcProfile

  import profile.api._
  // NOTE: GetResult mappers for plain SQL are only generated for tables where Slick knows how to map the types of all columns.
  import slick.jdbc.{GetResult => GR}

  /** DDL for all tables. Call .create to execute. */
  lazy val schema: profile.SchemaDescription = VoyageManifestPassengerInfo.schema ++ ProcessedJson.schema ++ ProcessedZip.schema

  case class ProcessedZipRow(zip_file_name: String, success: Boolean, processed_at: Timestamp)

  case class ProcessedJsonRow(zip_file_name: String, json_file_name: String, suspicious_date: Boolean, success: Boolean, processed_at: Timestamp)

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
                                            jsonFile: String)

  /** GetResult implicit for fetching ArrivalRow objects using plain SQL queries */
  implicit def GetResultVoyageManifestPassengerInfoRow(implicit e0: GR[String], e1: GR[java.sql.Timestamp], e2: GR[Int]): GR[VoyageManifestPassengerInfoRow] = GR {
    prs =>
      import prs._
      VoyageManifestPassengerInfoRow.tupled((<<[String], <<[String], <<[String], <<[Int], <<[String], <<[java.sql.Timestamp], <<[Int], <<[Int], <<[String], <<[String], <<[String], <<[Int], <<[String], <<[String], <<[String], <<[String], <<[String], <<[Boolean], <<[String]))
  }

  private val maybeSchema = profile match {
    case _: PostgresProfile =>
      Some("public")
    case _ =>
      None
  }

  class ProcessedZip(_tableTag: Tag) extends profile.api.Table[ProcessedZipRow](_tableTag, maybeSchema, "processed_zip") {
    def * = (zip_file_name, success, processed_at) <> (ProcessedZipRow.tupled, ProcessedZipRow.unapply)

    val zip_file_name: Rep[String] = column[String]("zip_file_name")
    val success: Rep[Boolean] = column[Boolean]("success")
    val processed_at: Rep[Timestamp] = column[Timestamp]("processed_at")
  }

  class ProcessedJson(_tableTag: Tag) extends profile.api.Table[ProcessedJsonRow](_tableTag, maybeSchema, "processed_json") {
    def * = (zip_file_name, json_file_name, suspicious_date, success, processed_at) <> (ProcessedJsonRow.tupled, ProcessedJsonRow.unapply)

    val zip_file_name: Rep[String] = column[String]("zip_file_name")
    val json_file_name: Rep[String] = column[String]("json_file_name")
    val suspicious_date: Rep[Boolean] = column[Boolean]("suspicious_date")
    val success: Rep[Boolean] = column[Boolean]("success")
    val processed_at: Rep[Timestamp] = column[Timestamp]("processed_at")
  }

  /** Table description of table arrival. Objects of this class serve as prototypes for rows in queries. */
  class VoyageManifestPassengerInfo(_tableTag: Tag) extends profile.api.Table[VoyageManifestPassengerInfoRow](_tableTag, maybeSchema, "voyage_manifest_passenger") {
    def * = (event_code, arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled_date, day_of_week, week_of_year, document_type, document_issuing_country_code, eea_flag, age, disembarkation_port_code, in_transit_flag, disembarkation_port_country_code, nationality_country_code, passenger_identifier, in_transit, json_file) <> (VoyageManifestPassengerInfoRow.tupled, VoyageManifestPassengerInfoRow.unapply)

    val event_code: Rep[String] = column[String]("event_code")
    val arrival_port_code: Rep[String] = column[String]("arrival_port_code")
    val departure_port_code: Rep[String] = column[String]("departure_port_code")
    val voyage_number: Rep[Int] = column[Int]("voyage_number")
    val carrier_code: Rep[String] = column[String]("carrier_code")
    val scheduled_date: Rep[java.sql.Timestamp] = column[java.sql.Timestamp]("scheduled_date")
    val day_of_week: Rep[Int] = column[Int]("day_of_week")
    val week_of_year: Rep[Int] = column[Int]("week_of_year")
    val document_type: Rep[String] = column[String]("document_type")
    val document_issuing_country_code: Rep[String] = column[String]("document_issuing_country_code")
    val eea_flag: Rep[String] = column[String]("eea_flag")
    val age: Rep[Int] = column[Int]("age")
    val disembarkation_port_code: Rep[String] = column[String]("disembarkation_port_code")
    val in_transit_flag: Rep[String] = column[String]("in_transit_flag")
    val disembarkation_port_country_code: Rep[String] = column[String]("disembarkation_port_country_code")
    val nationality_country_code: Rep[String] = column[String]("nationality_country_code")
    val passenger_identifier: Rep[String] = column[String]("passenger_identifier")
    val in_transit: Rep[Boolean] = column[Boolean]("in_transit")
    val json_file: Rep[String] = column[String]("json_file")
  }

  /** Collection-like TableQuery object for table VoyageManifestPassengerInfo */
  lazy val VoyageManifestPassengerInfo = new TableQuery(tag => new VoyageManifestPassengerInfo(tag))
  lazy val ProcessedJson = new TableQuery(tag => new ProcessedJson(tag))
  lazy val ProcessedZip = new TableQuery(tag => new ProcessedZip(tag))
}

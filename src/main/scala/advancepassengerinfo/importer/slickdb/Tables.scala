package advancepassengerinfo.importer.slickdb

import drtlib.SDate
import slick.jdbc.PostgresProfile
import slick.sql.SqlProfile.ColumnOption.SqlType

import java.sql.{Date, Timestamp}
import scala.util.matching.Regex



case class ProcessedZipRow(zip_file_name: String, success: Boolean, processed_at: Timestamp, created_on: Option[String])

object ProcessedZipRow {
  private val dqFileNameDateRegex: Regex = "drt_dq_([0-9]{2})([0-9]{2})([0-9]{2})_[0-9]{6}_[0-9]{4}\\.zip".r

  def extractCreatedOn(fileName: String): Option[Date] = fileName match {
    case dqFileNameDateRegex(year, month, day) =>
      println(s"year: $year, month: $month, day: $day")
      Option(new Date(SDate(s"20$year-$month-$day").millisSinceEpoch))
    case _ => None
  }
}

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


/** Slick data model trait for extension, choice of backend or usage in the cake pattern. (Make sure to initialize this late.) */
trait Tables {
  val profile: slick.jdbc.JdbcProfile

  import profile.api._
  // NOTE: GetResult mappers for plain SQL are only generated for tables where Slick knows how to map the types of all columns.

  /** DDL for all tables. Call .create to execute. */
  lazy val schema: profile.SchemaDescription = VoyageManifestPassengerInfo.schema ++ ProcessedJson.schema ++ ProcessedZip.schema

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

  private val maybeSchema = profile match {
    case _: PostgresProfile =>
      Some("public")
    case _ =>
      None
  }

  class ProcessedZip(_tableTag: Tag) extends profile.api.Table[ProcessedZipRow](_tableTag, maybeSchema, "processed_zip") {
    val zip_file_name: Rep[String] = column[String]("zip_file_name")
    val success: Rep[Boolean] = column[Boolean]("success")
    val processed_at: Rep[Timestamp] = column[Timestamp]("processed_at")
    val created_on: Rep[Option[String]] = column[Option[String]]("created_on", SqlType("VARCHAR(10)"))

    def * = {
      val apply = ProcessedZipRow.apply _
      (zip_file_name, success, processed_at, created_on) <> (apply.tupled, ProcessedZipRow.unapply)
    }
  }

  class ProcessedJson(_tableTag: Tag) extends profile.api.Table[ProcessedJsonRow](_tableTag, maybeSchema, "processed_json") {
    val zip_file_name: Rep[String] = column[String]("zip_file_name")
    val json_file_name: Rep[String] = column[String]("json_file_name")
    val suspicious_date: Rep[Boolean] = column[Boolean]("suspicious_date")
    val success: Rep[Boolean] = column[Boolean]("success")
    val processed_at: Rep[Timestamp] = column[Timestamp]("processed_at")
    val arrival_port_code: Rep[Option[String]] = column[String]("arrival_port_code")
    val departure_port_code: Rep[Option[String]] = column[String]("departure_port_code")
    val voyage_number: Rep[Option[Int]] = column[Int]("voyage_number")
    val scheduled: Rep[Option[Timestamp]] = column[Timestamp]("scheduled_date")
    val event_code: Rep[Option[String]] = column[String]("event_code")
    val non_interactive_total_count: Rep[Option[Int]] = column[Int]("non_interactive_total_count")
    val non_interactive_trans_count: Rep[Option[Int]] = column[Int]("non_interactive_trans_count")
    val interactive_total_count: Rep[Option[Int]] = column[Int]("interactive_total_count")
    val interactive_trans_count: Rep[Option[Int]] = column[Int]("interactive_trans_count")

    def * = (zip_file_name, json_file_name, suspicious_date, success, processed_at,
      arrival_port_code, departure_port_code, voyage_number, scheduled,
      event_code, non_interactive_total_count, interactive_total_count, non_interactive_trans_count, interactive_trans_count) <> (ProcessedJsonRow.tupled, ProcessedJsonRow.unapply)
  }

  /** Table description of table arrival. Objects of this class serve as prototypes for rows in queries. */
  class VoyageManifestPassengerInfo(_tableTag: Tag) extends profile.api.Table[VoyageManifestPassengerInfoRow](_tableTag, maybeSchema, "voyage_manifest_passenger_info") {
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

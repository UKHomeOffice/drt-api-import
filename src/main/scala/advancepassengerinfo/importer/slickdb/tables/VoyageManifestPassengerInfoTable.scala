package advancepassengerinfo.importer.slickdb.tables

import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import slick.lifted.{Rep, Tag}

import java.sql.Timestamp

/** Table description of table arrival. Objects of this class serve as prototypes for rows in queries. */
class VoyageManifestPassengerInfoTable(_tableTag: Tag) extends Table[VoyageManifestPassengerInfoRow](_tableTag, Option("public"), "voyage_manifest_passenger_info") {
  def * = (event_code, arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled_date, day_of_week, week_of_year, document_type, document_issuing_country_code, eea_flag, age, disembarkation_port_code, in_transit_flag, disembarkation_port_country_code, nationality_country_code, passenger_identifier, in_transit, json_file) <> (VoyageManifestPassengerInfoRow.tupled, VoyageManifestPassengerInfoRow.unapply)

  val event_code: Rep[String] = column[String]("event_code")
  val arrival_port_code: Rep[String] = column[String]("arrival_port_code")
  val departure_port_code: Rep[String] = column[String]("departure_port_code")
  val voyage_number: Rep[Int] = column[Int]("voyage_number")
  val carrier_code: Rep[String] = column[String]("carrier_code")
  val scheduled_date: Rep[java.sql.Timestamp] = column[Timestamp]("scheduled_date")
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

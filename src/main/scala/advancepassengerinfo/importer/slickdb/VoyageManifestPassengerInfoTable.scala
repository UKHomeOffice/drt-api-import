package advancepassengerinfo.importer.slickdb

import advancepassengerinfo.manifests.{PassengerInfo, VoyageManifest}

import java.sql.Timestamp
import scala.concurrent.ExecutionContext
import scala.util.Try


case class VoyageManifestPassengerInfoTable(tables: Tables) {
  import tables.profile.api._
  import tables.{VoyageManifestPassengerInfo, VoyageManifestPassengerInfoRow}

  def rowsToInsert(vm: VoyageManifest, dayOfWeek: Int, weekOfYear: Int, jsonFile: String)
                  (implicit ec: ExecutionContext): (Int, DBIOAction[Unit, NoStream, Effect.Write]) = {
    val rows = voyageManifestRows(vm, dayOfWeek, weekOfYear, jsonFile)
    (rows.size, DBIO.seq(VoyageManifestPassengerInfo ++= rows))
  }

  def voyageManifestRows(vm: VoyageManifest, dayOfWeek: Int, weekOfYear: Int, jsonFile: String): List[VoyageManifestPassengerInfoRow] = {
    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    vm.bestPassengers.map { passenger => passengerRow(vm, dayOfWeek, weekOfYear, schTs, passenger, jsonFile) }
  }

  def passengerRow(vm: VoyageManifest, dayOfWeek: Int, weekOfYear: Int, schTs: Timestamp, p: PassengerInfo, jsonFile: String): tables.VoyageManifestPassengerInfoRow = {
    VoyageManifestPassengerInfoRow(
      event_code = vm.EventCode,
      arrival_port_code = vm.ArrivalPortCode,
      departure_port_code = vm.DeparturePortCode,
      voyage_number = vm.VoyageNumber.toInt,
      carrier_code = vm.CarrierCode,
      scheduled_date = schTs,
      day_of_week = dayOfWeek,
      week_of_year = weekOfYear,
      document_type = p.DocumentType.getOrElse(""),
      document_issuing_country_code = p.DocumentIssuingCountryCode,
      eea_flag = p.EEAFlag,
      age = p.Age.flatMap(maybeAge => Try(maybeAge.toInt).toOption).getOrElse(-1),
      disembarkation_port_code = p.DisembarkationPortCode.getOrElse(""),
      in_transit_flag = p.InTransitFlag,
      disembarkation_port_country_code = p.DisembarkationPortCountryCode.getOrElse(""),
      nationality_country_code = p.NationalityCountryCode.getOrElse(""),
      passenger_identifier = p.PassengerIdentifier.getOrElse(""),
      in_transit = p.InTransitFlag match {
        case "Y" => true
        case _ => false
      },
      jsonFile = jsonFile
    )
  }

  def dayOfWeekAndWeekOfYear(date: Timestamp)(implicit ec: ExecutionContext): DBIOAction[Option[(Int, Int)], NoStream, Effect] =
    sql"""SELECT EXTRACT(DOW FROM TIMESTAMP'#$date'), EXTRACT(WEEK FROM TIMESTAMP'#$date')""".as[(Int, Int)].map(_.headOption)
}

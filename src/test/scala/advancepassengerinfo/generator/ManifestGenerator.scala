package advancepassengerinfo.generator

import advancepassengerinfo.manifests.{PassengerInfo, VoyageManifest}

object ManifestGenerator {
  def passengerInfo(): PassengerInfo = PassengerInfo(
    DocumentType = Option("P"),
    DocumentIssuingCountryCode = "GBR",
    EEAFlag = "Y",
    Age = Option("22"),
    DisembarkationPortCode = Option("LHR"),
    InTransitFlag = "N",
    DisembarkationPortCountryCode = Option("GBR"),
    NationalityCountryCode = Option("GBR"),
    PassengerIdentifier = Option("123")
  )

  def manifest(date: String, time: String): VoyageManifest = VoyageManifest(
    "DC", "LHR", "JFK", "1000", "BA", date, time, List(passengerInfo())
  )
}

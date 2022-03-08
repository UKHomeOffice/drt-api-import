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

  def manifest(): VoyageManifest = VoyageManifest(
    "DC", "LHR", "JFK", "1000", "BA", "2022-06-01", "10:00", List(passengerInfo())
  )
}

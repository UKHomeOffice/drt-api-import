package advancepassengerinfo.manifests

case class PassengerInfo(DocumentType: Option[String],
                         DocumentIssuingCountryCode: String,
                         EEAFlag: String,
                         Age: Option[String] = None,
                         DisembarkationPortCode: Option[String],
                         InTransitFlag: String = "N",
                         DisembarkationPortCountryCode: Option[String] = None,
                         NationalityCountryCode: Option[String] = None,
                         PassengerIdentifier: Option[String]
                            )

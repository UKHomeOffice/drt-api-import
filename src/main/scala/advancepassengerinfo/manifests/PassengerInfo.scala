package advancepassengerinfo.manifests

case class PassengerInfo(DocumentType: Option[String],
                         DocumentIssuingCountryCode: String,
                         EEAFlag: String,
                         Age: Option[String],
                         DisembarkationPortCode: Option[String],
                         InTransitFlag: String,
                         DisembarkationPortCountryCode: Option[String],
                         NationalityCountryCode: Option[String],
                         PassengerIdentifier: Option[String],
                        )

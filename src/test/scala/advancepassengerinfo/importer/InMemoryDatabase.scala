package advancepassengerinfo.importer

import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonTable, ProcessedZipTable, VoyageManifestPassengerInfoTable}

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object InMemoryDatabase extends Db {

  object H2Tables extends {
    val profile = slick.jdbc.H2Profile
  }

  val profile = H2Tables.profile
  val con: profile.backend.Database = profile.backend.Database.forConfig("tsql.db")

  import profile.api._

  def dropAndCreateTables = Await.result(
    con.run(DBIO.seq(
      TableQuery[ProcessedZipTable].schema.dropIfExists,
      TableQuery[ProcessedZipTable].schema.create,
      TableQuery[ProcessedJsonTable].schema.dropIfExists,
      TableQuery[ProcessedJsonTable].schema.create,
      TableQuery[VoyageManifestPassengerInfoTable].schema.dropIfExists,
      TableQuery[VoyageManifestPassengerInfoTable].schema.create,
    )
    ), 1.second
  )
}

package advancepassengerinfo.importer

import advancepassengerinfo.importer.slickdb.Tables

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object InMemoryDatabase extends Db {

  object H2Tables extends {
    val profile = slick.jdbc.H2Profile
  } with Tables

  val tables: H2Tables.type = H2Tables
  val con: tables.profile.backend.Database = tables.profile.backend.Database.forConfig("tsql.db")

  def truncateDb(): Unit = {
    import tables.profile.api._
    tables.schema.truncateStatements.foreach { statement =>
      Await.ready(con.run(sqlu"""DELETE FROM processed_json"""), 1.second)
      Await.ready(con.run(sqlu"""DELETE FROM processed_zip"""), 1.second)
      Await.ready(con.run(sqlu"""DELETE FROM voyage_manifest_passenger_info"""), 1.second)
    }
  }
}

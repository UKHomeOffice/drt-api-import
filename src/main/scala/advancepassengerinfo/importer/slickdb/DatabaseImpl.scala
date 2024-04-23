package advancepassengerinfo.importer.slickdb

import slick.dbio.{DBIOAction, NoStream}

import scala.concurrent.Future

object DatabaseImpl {
  val profile = slick.jdbc.PostgresProfile

  private val db: profile.backend.Database = profile.api.Database.forConfig("db")

  def run[T]: DBIOAction[T, NoStream, Nothing] => Future[T] = db.run
}

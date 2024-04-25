package advancepassengerinfo.importer.slickdb.dao

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.DatabaseImpl.profile.api._
import advancepassengerinfo.importer.slickdb.tables.{ProcessedZipRow, ProcessedZipTable}

import scala.concurrent.Future


trait ProcessedZipDao {
  def insert(row: ProcessedZipRow): Future[Unit]

  def lastPersistedFileName: Future[Option[String]]

  def delete(zipFileName: String): Future[Int]

  def oldestDate: Future[Option[String]]
}

case class ProcessedZipDaoImpl(db: Db) extends ProcessedZipDao {
  private val table = TableQuery[ProcessedZipTable]

  override def insert(row: ProcessedZipRow): Future[Unit] = db.run(DBIO.seq(table += row))

  override def lastPersistedFileName: Future[Option[String]] = {
    val sourceFileNamesQuery = table.map(_.zip_file_name)
    db.run(sourceFileNamesQuery.max.result)
  }

  override def delete(zipFileName: String): Future[Int] = {
    val query = table.filter(_.zip_file_name === zipFileName).delete
    db.run(query)
  }

  override def oldestDate: Future[Option[String]] = {
    val query = table.map(_.created_on).min
    db.run(query.result)
  }
}


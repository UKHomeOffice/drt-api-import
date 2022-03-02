package advancepassengerinfo.importer.persistence

import advancepassengerinfo.importer.Db
import advancepassengerinfo.importer.slickdb.VoyageManifestPassengerInfoTable
import advancepassengerinfo.manifests.VoyageManifest
import akka.NotUsed
import akka.stream.scaladsl.Source
import drtlib.SDate
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


case class ManifestPersistor(db: Db, parallelism: Int)(implicit ec: ExecutionContext) {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  log.info(s"parallelism level: $parallelism")

  private val dqRegex: Regex = "drt_dq_([0-9]{2})([0-9]{2})([0-9]{2})_[0-9]{6}_[0-9]{4}\\.zip".r

  private val manifestTable = VoyageManifestPassengerInfoTable(db.tables)
  private val oneDayMillis: Long = 60 * 60 * 24 * 1000L

  import db.tables.profile.api._

  def zipFileDate(fileName: String): Option[SDate] = fileName match {
    case dqRegex(year, month, day) => Option(SDate(s"20$year-$month-$day"))
    case _ => None
  }

  def addPersistenceToStream(zipTries: Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed]): Source[Int, NotUsed] = zipTries
    .mapConcat {
      case (zipFileName, Failure(t)) =>
        log.error(s"Recording a failed zip", t)
        Await.ready(persistProcessedZipRecord(zipFileName, success = false), 5 second)
        List()
      case (zipFileName, Success(manifestTries)) =>
        Await.ready(persistProcessedZipRecord(zipFileName, success = true), 5 second)
        manifestTries
          .map {
            case (jsonFile, Failure(t)) =>
              log.error(s"Recording a failed json file", t)
              Await.ready(persistFailedJsonRecord(zipFileName, jsonFile), 5 second)
              None
            case (jsonFile, Success(manifest)) =>
              Option(zipFileName, jsonFile, manifest)
          }
          .collect {
            case Some(tuple) => tuple
          }
    }
    .mapAsync(parallelism) {
      case (zipFile, jsonFile, vm) => addDayOfWeekAndWeekOfYear(zipFile, jsonFile, vm)
    }
    .mapAsync(parallelism) {
      case (zipFile, jsonFile, vm, dow, woy) => persistManifest(zipFile, jsonFile, vm, dow, woy)
    }

  val con: db.tables.profile.backend.DatabaseDef = db.con

  def persistManifest(zf: String, jf: String, vm: VoyageManifest, dow: Int, woy: Int): Future[Int] = con
    .run(manifestTable.rowsToInsert(vm, dow, woy, jf)._2)
    .flatMap { _ => persistProcessedJsonRecord(zf, jf, vm) }

  def persistProcessedJsonRecord(zf: String, jf: String, vm: VoyageManifest): Future[Int] = {
    val suspiciousDate: Boolean = scheduledIsSuspicious(zf, vm)
    val processedAt = new Timestamp(SDate.now().millisSinceEpoch)
    val processedJsonFileToInsert = db.tables.ProcessedJson += db.tables.ProcessedJsonRow(zf, jf, suspiciousDate, success = true, processedAt)
    con.run(processedJsonFileToInsert)
  }

  def persistFailedJsonRecord(zf: String, jf: String): Future[Int] = {
    val processedAt = new Timestamp(SDate.now().millisSinceEpoch)
    val processedJsonFileToInsert = db.tables.ProcessedJson += db.tables.ProcessedJsonRow(zf, jf, suspicious_date = false, success = false, processedAt)
    con.run(processedJsonFileToInsert)
  }

  def persistProcessedZipRecord(zf: String, success: Boolean): Future[Int] = {
    val processedAt = new Timestamp(SDate.now().millisSinceEpoch)
    val processedZipFileToInsert = db.tables.ProcessedZip += db.tables.ProcessedZipRow(zf, success, processedAt)
    con.run(processedZipFileToInsert)
  }

  def scheduledIsSuspicious(zf: String, vm: VoyageManifest): Boolean = {
    val maybeSuspiciousDate: Option[Boolean] = for {
      zipDate <- zipFileDate(zf)
      scdDate <- vm.scheduleArrivalDateTime
    } yield {
      scdDate.millisSinceEpoch - zipDate.millisSinceEpoch > 2 * oneDayMillis
    }

    maybeSuspiciousDate.getOrElse(false)
  }

  def addDayOfWeekAndWeekOfYear(zipFile: String, jsonFile: String, vm: VoyageManifest): Future[(String, String, VoyageManifest, Int, Int)] = {
    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))

    con.run(manifestTable.dayOfWeekAndWeekOfYear(schTs)).collect {
      case Some((dow, woy)) => (zipFile, jsonFile, vm, dow, woy)
    }
  }

  def lastPersistedFileName: Future[Option[String]] = {
    val sourceFileNamesQuery = db.tables.ProcessedJson.map(_.zip_file_name)
    con.run(sourceFileNamesQuery.max.result)
  }
}

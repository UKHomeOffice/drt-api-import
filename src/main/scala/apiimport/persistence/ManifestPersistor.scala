package apiimport.persistence

import java.sql.Timestamp

import akka.NotUsed
import akka.stream.scaladsl.Source
import apiimport.Db
import apiimport.manifests.VoyageManifestParser.VoyageManifest
import apiimport.slickdb.VoyageManifestPassengerInfoTable
import drtlib.SDate
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}


case class ManifestPersistor(db: Db)(implicit ec: ExecutionContext) {
  val log: Logger = LoggerFactory.getLogger(getClass)

  val dqRegex: Regex = "drt_dq_([0-9]{2})([0-9]{2})([0-9]{2})_[0-9]{6}_[0-9]{4}\\.zip".r

  val manifestTable = VoyageManifestPassengerInfoTable(db.tables)

  import db.tables.profile.api._

  def zipFileDate(fileName: String): Option[SDate] = fileName match {
    case dqRegex(year, month, day) => Option(SDate(s"20$year-$month-$day"))
    case _ => None
  }

  val oneDayMillis: Long = 60 * 60 * 24 * 1000L

  def addPersistence(zipTries: Source[(String, Try[List[(String, Try[VoyageManifest])]]), NotUsed]): Source[Seq[Int], NotUsed] = zipTries
    .mapConcat {
      case (zipFile, Failure(t)) =>
        log.error(s"Recording a failed zip", t)
        Await.ready(persistProcessedZipRecord(zipFile, success = false), 1 second)
        List()
      case (zipFile, Success(manifestTries)) => manifestTries
        .map {
          case (jsonFile, Failure(t)) =>
            log.error(s"Recording a failed json file", t)
            Await.ready(persistFailedJsonRecord(zipFile, jsonFile), 1 second)
            None
          case (jsonFile, Success(manifest)) =>
            Option(zipFile, jsonFile, manifest)
        }
        .collect {
          case Some(tuple) => tuple
        }
    }
    .mapAsync(12) {
      case (zipFile, jsonFile, vm) => addDowWoy(zipFile, jsonFile, vm)
    }
    .mapAsync(6) {
      case (zipFile, jsonFile, vm, dow, woy) =>
        removeExisting(zipFile, jsonFile, vm, dow, woy)
          .flatMap(_ => Future.sequence(Seq(
            persistManifest(zipFile, jsonFile, vm, dow, woy),
            persistProcessedZipRecord(zipFile, success = true))))
    }

  val con: db.tables.profile.backend.DatabaseDef = db.con

  def persistManifest(zf: String, jf: String, vm: VoyageManifest, dow: Int, woy: Int): Future[Int] = con
    .run(manifestTable.rowsToInsert(vm, dow, woy, jf))
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

  def removeExisting(zf: String, jf: String, vm: VoyageManifest, dow: Int, woy: Int): Future[(String, String, VoyageManifest, Int, Int)] = {
    val schTs = new Timestamp(vm.scheduleArrivalDateTime.map(_.millisSinceEpoch).getOrElse(0L))
    val value = db.tables.VoyageManifestPassengerInfo.filter(r => {
      r.event_code === vm.EventCode &&
        r.arrival_port_code === vm.ArrivalPortCode &&
        r.departure_port_code === vm.DeparturePortCode &&
        r.scheduled_date === schTs &&
        r.voyage_number === vm.VoyageNumber.toInt
    })

    con.run(value.delete).map(deletedCount => {
      if (deletedCount > 0) log.info(s"Removed $deletedCount existing entries")
      (zf, jf, vm, dow, woy)
    })
  }

  def addDowWoy(zipFile: String, jsonFile: String, vm: VoyageManifest): Future[(String, String, VoyageManifest, Int, Int)] = {
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

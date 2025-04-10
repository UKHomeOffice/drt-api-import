package advancepassengerinfo.importer.processor

import advancepassengerinfo.importer.provider.Manifests
import advancepassengerinfo.importer.slickdb.dao.{ProcessedJsonDao, ProcessedZipDao, VoyageManifestPassengerInfoDao}
import advancepassengerinfo.importer.slickdb.serialisation.VoyageManifestSerialisation.voyageManifestRows
import advancepassengerinfo.importer.slickdb.tables.{ProcessedJsonRow, ProcessedZipRow}
import advancepassengerinfo.manifests.VoyageManifest
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import com.typesafe.scalalogging.Logger
import drtlib.SDate

import java.sql.Timestamp
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}


trait DqFileProcessor {
  def process(zipFileName: String): Source[Option[(Int, Int)], Any]
}

case class DqFileProcessorImpl(manifestsProvider: Manifests,
                               zipDao: ProcessedZipDao,
                               jsonDao: ProcessedJsonDao,
                               manifestsDao: VoyageManifestPassengerInfoDao,
                              )
                              (implicit ec: ExecutionContext) extends DqFileProcessor {
  private val log = Logger(getClass)

  private val oneDayMillis: Long = 1.day.toMillis

  def process(zipFileName: String): Source[Option[(Int, Int)], Any] = {
    val processedAtTs = new Timestamp(SDate.now().millisSinceEpoch)
    val maybeCreatedOn = ProcessedZipRow.extractCreatedOn(zipFileName)

    manifestsProvider.tryManifests(zipFileName).flatMapConcat {
        case Success(jsonFileNamesWithManifests) =>
          persistManifests(zipFileName, jsonFileNamesWithManifests)
            .mapAsync(1) { case (total, successful) =>
              val row = ProcessedZipRow(zipFileName, successful > 0, processedAtTs, maybeCreatedOn)
              zipDao.insert(row).map(_ => Option((total, successful)))
            }
            .recover {
              case t =>
                log.error(s"Failed to persist zip file $zipFileName: ${t.getMessage}")
                None
            }

        case Failure(throwable) =>
          log.error(s"Failed to process zip file $zipFileName: ${throwable.getMessage}")
          val row = ProcessedZipRow(zipFileName, success = false, processedAtTs, maybeCreatedOn)
          Source
            .future(zipDao.insert(row))
            .map(_ => None)
            .recover {
              case t =>
                log.error(s"Failed to persist zip file $zipFileName: ${t.getMessage}")
                None
            }
      }
      .recover {
        case t =>
          log.error(s"Failed to process files after $zipFileName: ${t.getMessage}")
          None
      }
  }

  private def persistManifests(zipFileName: String, manifestTries: Seq[(String, Try[VoyageManifest])]): Source[(Int, Int), NotUsed] =
    Source(manifestTries)
      .foldAsync((0, 0)) {
        case ((total, success), (jsonFileName, tryManifest)) =>
          val processedAt = SDate.now().millisSinceEpoch
          jsonDao.jsonHasBeenProcessed(zipFileName, jsonFileName).flatMap { alreadyProcessed =>
              (alreadyProcessed, tryManifest) match {
                case (true, _) =>
                  log.warn(s"Skipping $jsonFileName as it's already been processed as part of $zipFileName")
                  Future.successful((total, success))
                case (false, Failure(exception)) =>
                  log.error(s"Failed to extract manifest from $jsonFileName in $zipFileName: ${exception.getMessage}")
                  persistFailedJson(zipFileName, jsonFileName, processedAt).map(_ => (total + 1, success))

                case (false, Success(manifest)) =>
                  persistManifest(zipFileName, total, success, jsonFileName, processedAt, manifest)
              }
            }
            .recover {
              case t =>
                log.error(s"Failed to persist manifest from $jsonFileName in $zipFileName: ${t.getMessage}")
                (total + 1, success)
            }
      }

  private def persistManifest(zipFileName: String,
                              total: Int,
                              success: Int,
                              jsonFileName: String,
                              processedAt: Long,
                              manifest: VoyageManifest,
                             ): Future[(Int, Int)] = {
    manifest.scheduleArrivalDateTime match {
      case Some(scheduled) =>
        manifestsDao.dayOfWeekAndWeekOfYear(new Timestamp(scheduled.millisSinceEpoch))
          .flatMap {
            case (dayOfWeek, weekOfYear) =>
              val manifestRows = voyageManifestRows(manifest, dayOfWeek, weekOfYear, jsonFileName)
              manifestsDao.insert(manifestRows)
          }
          .flatMap { _ =>
            persistSuccessfulJson(zipFileName, jsonFileName, manifest, processedAt).map(_ => (total + 1, success + 1))
          }
          .recoverWith {
            case t =>
              log.error(s"Failed to persist manifest from $jsonFileName in $zipFileName: ${t.getMessage}")
              persistFailedJson(zipFileName, jsonFileName, processedAt).map(_ => (total + 1, success))
          }
      case None =>
        log.error(s"Failed to get a scheduled time for ${manifest.DeparturePortCode} > ${manifest.ArrivalPortCode} " +
          s":: ${manifest.CarrierCode}-${manifest.VoyageNumber} :: ${manifest.ScheduledDateOfArrival}T${manifest.ScheduledDateOfArrival}")
        persistFailedJson(zipFileName, jsonFileName, processedAt).map(_ => (total + 1, success))
    }
  }

  private def persistSuccessfulJson(zipFileName: String, jsonFileName: String, manifest: VoyageManifest, processedAt: Long): Future[Unit] = {
    val isSuspicious = scheduledIsSuspicious(zipFileName, manifest)
    val row = ProcessedJsonRow.fromManifest(zipFileName, jsonFileName, successful = true, dateIsSuspicious = isSuspicious, Option(manifest), processedAt)
    jsonDao.insert(row)
  }

  private def persistFailedJson(zipFileName: String, jsonFileName: String, processedAt: Long): Future[Unit] = {
    val row = ProcessedJsonRow.fromManifest(zipFileName, jsonFileName, successful = false, dateIsSuspicious = false, None, processedAt)
    jsonDao.insert(row)
  }


  private def scheduledIsSuspicious(zf: String, vm: VoyageManifest): Boolean = {
    val maybeSuspiciousDate: Option[Boolean] = for {
      zipDate <- ProcessedZipRow.extractCreatedOn(zf)
      scdDate <- vm.scheduleArrivalDateTime
    } yield {
      scdDate.millisSinceEpoch - SDate(zipDate).millisSinceEpoch > 2 * oneDayMillis
    }

    maybeSuspiciousDate.getOrElse(false)
  }
}

/**
 *
 alter table processed_json add column arrival_port_code varchar(5);
 alter table processed_json add column departure_port_code varchar(5);
 alter table processed_json add column voyage_number integer;
 alter table processed_json add column carrier_code varchar(5);
 alter table processed_json add column scheduled timestamp without time zone;
 alter table processed_json add column event_code varchar(3);
 alter table processed_json add column non_interactive_total_count smallint;
 alter table processed_json add column non_interactive_trans_count smallint;
 alter table processed_json add column interactive_total_count smallint;
 alter table processed_json add column interactive_trans_count smallint;

 CREATE INDEX processed_json_unique_arrival ON public.processed_json (arrival_port_code, departure_port_code, scheduled, voyage_number, event_code);
 CREATE INDEX processed_json_route_day_week_year ON public.processed_json (arrival_port_code, departure_port_code, event_code, extract(week from scheduled), extract(year from scheduled));
 CREATE INDEX processed_json_arrival_week_year ON public.processed_json (arrival_port_code, departure_port_code, voyage_number, event_code, extract(week from scheduled), extract(year from scheduled));
 CREATE INDEX processed_json_arrival_day_week_year ON public.processed_json (arrival_port_code, departure_port_code, voyage_number, event_code, extract(dow from scheduled), extract(week from scheduled), extract(year from scheduled));
 CREATE INDEX processed_json_arrival_port_date_event_code ON public.processed_json (arrival_port_code, cast(scheduled as date), event_code);

 update processed_json pj
 set (arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled, event_code,
 non_interactive_total_count, non_interactive_trans_count, interactive_total_count, interactive_trans_count) = (
 select
 arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled_date, event_code,
 count(*) filter (where passenger_identifier='') as non_interactive_total_count,
 count(*) filter (where passenger_identifier='' and in_transit=true) as non_interactive_trans_count,
 count(*) filter (where passenger_identifier!='') as interactive_total_count,
 count(*) filter (where passenger_identifier!='' and in_transit=true) as interactive_trans_count
 from voyage_manifest_passenger_info vm
 where vm.json_file = pj.json_file_name
 group by arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled_date, event_code
 limit 1
 )
 from processed_zip pz
 where pj.zip_file_name = pz.zip_file_name and pz.created_on = '2022-04-07';

 alter table processed_zip add created_on varchar(10);
 create index processed_zip_created_on on processed_zip (created_on);
 update processed_zip
 set created_on = concat('20',substring(zip_file_name, 8, 2),'-',substring(zip_file_name, 10, 2),'-',substring(zip_file_name, 12, 2))::date
 where created_on is null;

select count(*) as dupes, json_file_name, arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled, event_code
 from processed_json pj
 inner join processed_zip pz on pj.zip_file_name = pz.zip_file_name
 where pz.created_on='2022-04-07'
 group by arrival_port_code, departure_port_code, voyage_number, carrier_code, scheduled, event_code, json_file_name
  having count(*) > 1;

 *
 * */

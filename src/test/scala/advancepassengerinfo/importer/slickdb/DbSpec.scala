package advancepassengerinfo.importer.slickdb

import advancepassengerinfo.importer.InMemoryDatabase
import advancepassengerinfo.importer.slickdb.dao.VoyageManifestPassengerInfoDaoImpl
import drtlib.SDate
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.sql.Timestamp
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.language.postfixOps


class DbSpec extends AnyWordSpec with Matchers with Builder {
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  private val manifestsDao = VoyageManifestPassengerInfoDaoImpl(InMemoryDatabase)

  "A request for day of the week" should {
    "give me 6 (for Saturday) when given a date falling on a Saturday" in {
      val date = new Timestamp(SDate("2019-04-20T12:00:00Z").millisSinceEpoch)
      val sql = manifestsDao.dayOfWeekAndWeekOfYear(date)
      val (dow, _) = Await.result(sql, 1 second)

      dow should be(6)
    }
  }

  "A request for day of the week" should {
    "give me 0 (for Sunday) when given a date falling on a Sunday" in {
      val date = new Timestamp(SDate("2019-04-21T12:00:00Z").millisSinceEpoch)
      val sql = manifestsDao.dayOfWeekAndWeekOfYear(date)
      val (dow, _) = Await.result(sql, 1 second)

      dow should be(0)
    }
  }

  "A request for week of the year" should {
    "give me 1 when given a date falling in the first week of the year (according to h2's definitions)" in {
      val date = new Timestamp(SDate("2019-01-01T12:00:00Z").millisSinceEpoch)
      val sql = manifestsDao.dayOfWeekAndWeekOfYear(date)
      val (_, woy)  = Await.result(sql, 1 second)

      woy should be(1)
    }
  }
}

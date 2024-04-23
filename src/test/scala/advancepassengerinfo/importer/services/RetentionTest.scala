package advancepassengerinfo.importer.services

import drtlib.SDate
import org.scalatest.concurrent.ScalaFutures.convertScalaFuture
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RetentionTest extends AnyWordSpec with Matchers {
  "isOlderThanRetentionThreshold" should {
    val retentionYears = 1
    val now = () => SDate("2021-01-01")
    val isOld = Retention.isOlderThanRetentionThreshold(retentionYears, now)

    "return true if the date is older than the retention threshold" in {
      isOld(SDate("2020-01-01")) shouldBe true
    }

    "return false if the date is not older than the retention threshold" in {
      isOld(SDate("2020-12-31")) shouldBe false
    }
  }

  "deleteOldData" should {
    "return a function that deletes the data if the oldest data is older than the retention threshold" in {
      val oldestData = () => Future.successful(Some(SDate("2020-01-01")))
      val deleteForDate = (_: SDate) => Future.successful((1, 2, 3))
      val delete = Retention.deleteOldData(oldestData, deleteForDate)
      delete().futureValue shouldBe (1, 2, 3)
    }

    "return a function that does not delete the data if the oldest data is not older than the retention threshold" in {
      val oldestData = () => Future.successful(None)
      val deleteForDate = (_: SDate) => Future.successful((1, 2, 3))
      val delete = Retention.deleteOldData(oldestData, deleteForDate)
      delete().futureValue shouldBe (0, 0, 0)
    }

    "return a function that logs an error if the deletion fails" in {
      val oldestData = () => Future.successful(Some(SDate("2020-01-01")))
      val deleteForDate = (_: SDate) => Future.failed(new Exception("Failed to delete data"))
      val delete = Retention.deleteOldData(oldestData, deleteForDate)
      delete().failed.futureValue.getMessage shouldBe "Failed to delete data"
    }
  }
}

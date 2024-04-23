package drtlib

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt

class SDateTest extends AnyWordSpec with Matchers {
  "dayOfWeek" should {
    "return 1 for Monday" in {
      SDate("2020-01-06").dayOfWeek shouldBe 1
    }
    "return 7 for Sunday" in {
      SDate("2020-01-12").dayOfWeek shouldBe 7
    }
  }

  "fullYear" should {
    "return 2020 for 2020-01-06" in {
      SDate("2020-01-06").fullYear shouldBe 2020
    }
  }

  "month" should {
    "return 1 for 2020-01-06" in {
      SDate("2020-01-06").month shouldBe 1
    }
  }

  "date" should {
    "return 6 for 2020-01-06" in {
      SDate("2020-01-06").date shouldBe 6
    }
  }

  "minute" should {
    "return 0 for 2020-01-06T00:00" in {
      SDate("2020-01-06T00:00").minute shouldBe 0
    }
  }

  "plus" should {
    "add 1 day to 2020-01-06" in {
      SDate("2020-01-06").plus(1.day) shouldBe SDate("2020-01-07")
    }
  }

  "minus" should {
    "subtract 1 day from 2020-01-06" in {
      SDate("2020-01-06").minus(1.day) shouldBe SDate("2020-01-05")
    }
  }

  "millisSinceEpoch" should {
    "return 1578268800000 for 2020-01-06" in {
      SDate("2020-01-06").millisSinceEpoch shouldBe 1578268800000L
    }
  }

  "<" should {
    "return true for 2020-01-06 < 2020-01-07" in {
      SDate("2020-01-06") < SDate("2020-01-07") shouldBe true
    }
    "return false for 2020-01-07 < 2020-01-06" in {
      SDate("2020-01-07") < SDate("2020-01-06") shouldBe false
    }
  }
}

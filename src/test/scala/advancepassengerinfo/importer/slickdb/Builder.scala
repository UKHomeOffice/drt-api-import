package advancepassengerinfo.importer.slickdb

import advancepassengerinfo.importer.InMemoryDatabase
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.language.postfixOps


trait Builder extends BeforeAndAfterEach {
  this: Suite =>

  override def beforeEach(): Unit = {
    InMemoryDatabase.dropAndCreateTables
  }

  override def afterEach(): Unit = super.afterEach()
}

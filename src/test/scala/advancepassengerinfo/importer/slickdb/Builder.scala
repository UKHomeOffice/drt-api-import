package advancepassengerinfo.importer.slickdb

import advancepassengerinfo.importer.InMemoryDatabase
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps


trait Builder extends BeforeAndAfterEach {
  this: Suite =>

  import advancepassengerinfo.importer.InMemoryDatabase.tables.profile.api._

  override def beforeEach(): Unit = {
    Await.ready(InMemoryDatabase.con.run(InMemoryDatabase.tables.schema.drop), 1 second)
    Await.ready(InMemoryDatabase.con.run(InMemoryDatabase.tables.schema.create), 1 second)
    super.beforeEach()
  }

  override def afterEach(): Unit = super.afterEach()
}

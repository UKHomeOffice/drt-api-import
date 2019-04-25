package apiimport.slickdb

import apiimport.H2Db
import apiimport.H2Db.H2Tables
import org.scalatest.{BeforeAndAfterEach, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._


trait Builder extends BeforeAndAfterEach {
  this: Suite =>

  import apiimport.H2Db.tables.profile.api._

  override def beforeEach(): Unit = {
    Await.ready(H2Db.con.run(H2Tables.schema.drop), 1 second)
    Await.ready(H2Db.con.run(H2Tables.schema.create), 1 second)
    super.beforeEach()
  }

  override def afterEach(): Unit = super.afterEach()
}

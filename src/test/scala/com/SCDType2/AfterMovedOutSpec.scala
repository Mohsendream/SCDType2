package com.SCDType2

import com.SCDType2.AfterMovedOut.afterMovedOut
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class history_1(Id: Long, firstName: String, lastName: String, address: String, movedIn: String, movedOut: String, status: Boolean)

case class updates_1(newId: Long, newFirstName: String, newLastName: String, newAddress: String, newMovedIn: String)

class AfterMovedOutSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("AfterMovedOutTest")
    .getOrCreate()

  import spark.implicits._

  "AfterMovedOut" should "return  1 Madiouni Mohsen france 21-06-2014 Null true" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(history_1(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", true)).toDF()
    val updatesDataframe = Seq(updates_1(1L, "Madiouni", "Mohsen", "France", "21-06-2014")).toDF()
    When("AfteMovedOut is invoked")
    val result = afterMovedOut(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(history_1(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", false),
      history_1(1L, "Madiouni", "Mohsen", "France", "21-06-2014", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

package com.SCDType2

import com.SCDType2.DifferentId.differentId
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class history(Id: Long, firstName: String, lastName: String, address: String, movedIn: String, movedOut: String, status: Boolean)

case class updates(newId: Long, newFirstName: String, newLastName: String, newAddress: String, newMovedIn: String)

class DifferentIdSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("AddressHistoryBuildeTest")
    .getOrCreate()

  import spark.implicits._

  "differentId" should "2L, \"Comblet\", \"Fabrice\", \"France\", \"06-07-2012\", \"Null\", true" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(history(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", true)).toDF()
    val updatesDataframe = Seq(updates(2L, "Comblet", "Fabrice", "France", "06-07-2012")).toDF()
    When("differentId is invoked")
    val result = differentId(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(history(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", true),
      history(2L, "Comblet", "Fabrice", "France", "06-07-2012", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

package com.SCDType2

import com.SCDType2.SameAddressAfterDates.sameAddressAfterDates
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SameAddressAfterDatesSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  import spark.implicits._

  "SameAddressAfterDatesSpec" should "return a history table that doesnt care about that case" in {
    Given("the history and the updates dataframes")
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Kef", "15-09-2014")).toDF()
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "10-08-2014", true)).toDF()
    val joinedDataFrame = historyDataframe.join(updatesDataframe, historyDataframe.col("Id") === updatesDataframe.col("newId"), "inner")
    When("duplicates is invoked")
    val result = sameAddressAfterDates (joinedDataFrame, historyDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "10-08-2014", false),
      History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2014", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
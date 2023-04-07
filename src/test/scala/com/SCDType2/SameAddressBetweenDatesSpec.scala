package com.SCDType2

import com.SCDType2.SameAddressBetweenDates.sameAddressBetweenDates
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SameAddressBetweenDatesSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  import spark.implicits._

  "sameAddressBetweenDatesSpec" should "drop the these cases from the update table" in {
    Given("the history and the updates dataframes")
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Kef", "15-09-2012"),
      Updates(2L, "Madiouni", "Haifa", "Liege", "15-09-2021"),
      Updates(3L, "Boukari", "Dorra", "Paris", "16-04-2023")).toDF()
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "Null", true),
      History(2L, "Madiouni", "Haifa", "Liege", "15-09-2020", "Null", true)).toDF()
    When("duplicates is invoked")
    val result = sameAddressBetweenDates(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(Updates(3L, "Boukari", "Dorra", "Paris", "16-04-2023")).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

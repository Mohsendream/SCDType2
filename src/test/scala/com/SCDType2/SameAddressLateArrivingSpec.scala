package com.SCDType2

import com.SCDType2.SameAddressLateArriving.sameAddressLateArriving
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SameAddressLateArrivingSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("test")
    .getOrCreate()

  import spark.implicits._

  "SameAddressLateArrivingSpec" should "return a history table updates with the first date when the same address" in {
    Given("the history and the updates dataframes")
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Kef", "15-09-2009")).toDF()
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "Null", true),
      History(2L, "Madiouni", "Haifa", "Liege", "15-09-2020", "Null", true)).toDF()
    val joinedDataFrame = historyDataframe.join(updatesDataframe, historyDataframe.col("Id") === updatesDataframe.col("newId"), "inner")
    When("duplicates is invoked")
    val result = sameAddressLateArriving(joinedDataFrame, historyDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2009", "Null", true),
      History(2L, "Madiouni", "Haifa", "Liege", "15-09-2020", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

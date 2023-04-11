package com.SCDType2

import com.SCDType2.BeforeMovedIn.beforeMovedIn
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers



class BeforeMovedInSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("AddressHistoryBuildeTest")
    .getOrCreate()

  import spark.implicits._

  "BeforeMovedInSpec" should "return  1 Madiouni Mohsen Bouarada 06-07-1995 15-09-2010 false" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "Null", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "bouarada", "06-07-1995")).toDF()
    When("BeforeMovedInSpec is invoked")
    val joinedDataFrame= historyDataframe.join(updatesDataframe, historyDataframe.col("Id") === updatesDataframe.col("newId"), "inner")
    val result = beforeMovedIn(joinedDataFrame, historyDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "Null", true),
      History(1L, "Madiouni", "Mohsen", "bouarada", "06-07-1995", "15-09-2010", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}
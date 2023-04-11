package com.SCDType2

import com.SCDType2.AddressHistoryBuilder.addressHistoryBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class History(Id: Long, firstName: String, lastName: String, address: String, movedIn: String, movedOut: String, status: Boolean)

case class Updates(newId: Long, newFirstName: String, newLastName: String, newAddress: String, newMovedIn: String)

class AddressHistoryBuilderSpec extends AnyFlatSpec with Matchers with GivenWhenThen {

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("AddressHistoryBuildeTest")
    .getOrCreate()

  import spark.implicits._

  "Test1" should "insert new records with different Ids" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", true)).toDF()
    val updatesDataframe = Seq(Updates(2L, "Comblet", "Fabrice", "France", "06-07-2012")).toDF()
    When("differentId is invoked")
    val result = addressHistoryBuilder(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", true),
      History(2L, "Comblet", "Fabrice", "France", "06-07-2012", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }

  "Test2" should "return the after movedOut correct" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "France", "21-06-2014")).toDF()
    When("differentId is invoked")
    val result = addressHistoryBuilder(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Kef", "15-09-2010", "21-06-2014", false),
      History(1L, "Madiouni", "Mohsen", "France", "21-06-2014", "Null", true)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
  "Test3" should "return  the between dates first test" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "21-06-2014", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012")).toDF()
    When("differentId is invoked")
    val result = addressHistoryBuilder(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012", "21-06-2014", true),
      History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "06-07-2012", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }

  "Test 4" should "the between dates second test" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "Null", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012")).toDF()
    When("differentId is invoked")
    val result = addressHistoryBuilder(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "Tunis", "06-07-2012", "Null", true),
      History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "06-07-2012", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }

  "Test5" should "return  before moved in date" in {
    Given("the history dataframe and the updates dataframe")
    val historyDataframe = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "Null", true)).toDF()
    val updatesDataframe = Seq(Updates(1L, "Madiouni", "Mohsen", "bouarada", "06-07-1995")).toDF()
    When("differentId is invoked")
    val result = addressHistoryBuilder(historyDataframe, updatesDataframe, spark)
    Then("the result should be returned")
    val expectedResult = Seq(History(1L, "Madiouni", "Mohsen", "kef", "15-09-2010", "Null", true),
      History(1L, "Madiouni", "Mohsen", "bouarada", "06-07-1995", "15-09-2010", false)).toDF()
    expectedResult.collect() should contain theSameElementsAs result.collect()
  }
}

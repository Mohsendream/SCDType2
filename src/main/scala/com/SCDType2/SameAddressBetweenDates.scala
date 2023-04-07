package com.SCDType2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, to_date}

object SameAddressBetweenDates {

  def sameAddressBetweenDates(historyDataframe: DataFrame, updatesDataframe: DataFrame, spark: SparkSession): DataFrame = {
    val inBetweenDatessameAddress = historyDataframe.join(updatesDataframe, historyDataframe.col("Id") === updatesDataframe.col("newId"), "inner")
      .where(col("newAddress") === col("Address"))
      .where((to_date(col("newMovedIn"), "dd-MM-yyyy") > to_date(col("movedIn"), "dd-MM-yyyy") and (to_date(col("newMovedIn"), "dd-MM-yyyy") <= to_date(col("movedOut"), "dd-MM-yyyy"))) or
        (to_date(col("newMovedIn"), "dd-MM-yyyy") > to_date(col("movedIn"), "dd-MM-yyyy") and col("movedOut") === "Null"))
    val updatedRecords = inBetweenDatessameAddress.select(col("newId"),
      col("newFirstName"), col("newLastName"),
      col("newAddress"), col("newMovedIn"))
    val newUpdates = updatesDataframe.except(updatedRecords)
    newUpdates
  }
}

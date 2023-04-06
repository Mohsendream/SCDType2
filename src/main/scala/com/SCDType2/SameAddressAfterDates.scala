package com.SCDType2

import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._

object SameAddressAfterDates {
  def sameAddressAfterDates(historyDataframe: DataFrame, updatesDataframe: DataFrame, spark: SparkSession):DataFrame={

    val inBetweenDatessameAddress = historyDataframe.join(updatesDataframe, historyDataframe.col("Id") === updatesDataframe.col("newId"), "inner")
      .where(col("newAddress") === col("Address"))
      .where(to_date(col("newMovedIn"), "dd-MM-yyyy") === to_date(col("movedOut"), "dd-MM-yyyy") or col("movedOut") === "Null")
    val updatedRecords = inBetweenDatessameAddress.select(col("newId"),
      col("newFirstName"), col("newLastName"),
      col("newAddress"), col("newMovedIn"))
    updatedRecords
  }
}

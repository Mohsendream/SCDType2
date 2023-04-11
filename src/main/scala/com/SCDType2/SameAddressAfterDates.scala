package com.SCDType2

import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._

object SameAddressAfterDates {

  def sameAddressAfterDates(joinedDataFrame: DataFrame, historyDataframe: DataFrame, spark: SparkSession):DataFrame={

    val inBetweenDatessameAddress = joinedDataFrame
      .where(col("newAddress") === col("Address"))
      .where(to_date(col("newMovedIn"), "dd-MM-yyyy") >= to_date(col("movedOut"), "dd-MM-yyyy"))
    val updatedRecords = inBetweenDatessameAddress.select(col("newId").as("Id"),
      col("newFirstName").as("firstName"), col("newLastName").as("lastName"),
      col("newAddress").as("address"), col("newMovedIn").as("movedIn"),
      col("movedOut"), col("status"))
      .withColumn("movedOut", lit("Null"))
      .withColumn("status", lit(true))
      .withColumn("movedIn", col("movedIn"))
    val updatedHistory = inBetweenDatessameAddress.select(col("Id"), col("firstName"), col("lastName"),
      col("Address"), col("movedIn"), col("movedOut"))
      .withColumn("status", lit(false))
    val bareHistory = historyDataframe.join(updatedHistory, historyDataframe.col("Id") === updatedHistory.col("Id"), "left_anti")
    val touchedHistory = updatedHistory.union(bareHistory).union(updatedRecords)
    touchedHistory
  }
}

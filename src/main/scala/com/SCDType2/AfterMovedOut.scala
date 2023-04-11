package com.SCDType2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AfterMovedOut {

  def afterMovedOut(joinedDataFrame: DataFrame, newHistory:DataFrame, spark: SparkSession): DataFrame = {
    val updatesWithDatesAfterMovedOut = joinedDataFrame
      .where(col("newAddress") =!= col("address"))
      .where(to_date(col("newMovedIn"), "dd-MM-yyyy") >= to_date(col("movedOut"), "dd-MM-yyyy"))
    val updatedRecords_1 = updatesWithDatesAfterMovedOut.select(col("newId").as("Id"),
      col("newFirstName").as("firstName"), col("newLastName").as("lastName"),
      col("newAddress").as("address"), col("newMovedIn").as("movedIn"))
      .withColumn("movedOut", lit("Null"))
      .withColumn("status", lit(true))
    val updatedHistory_1 = updatesWithDatesAfterMovedOut.select(col("Id"), col("firstName"), col("lastName"),
      col("Address"), col("movedIn"), col("movedOut"))
      .withColumn("status", lit(false))
    val bareHistory = newHistory.join(updatedHistory_1, newHistory.col("Id") === updatedHistory_1.col("Id"), "left_anti")
    val touchedHistory = updatedHistory_1.union(bareHistory).union(updatedRecords_1)
    touchedHistory
  }
}

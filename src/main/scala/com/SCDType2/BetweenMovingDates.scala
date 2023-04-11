package com.SCDType2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BetweenMovingDates {

  def betweenMovingDates( joinedDataFrame: DataFrame, touchedHistory: DataFrame, spark: SparkSession): DataFrame = {

    val updatesWithDatesBetweenMovedDates = joinedDataFrame
      .where(col("newAddress") =!= col("address"))
      .where((to_date(col("newMovedIn"), "dd-MM-yyyy") >= to_date(col("movedIn"), "dd-MM-yyyy") and (to_date(col("newMovedIn"), "dd-MM-yyyy") < to_date(col("movedOut"), "dd-MM-yyyy"))) or
        (to_date(col("newMovedIn"), "dd-MM-yyyy") >= to_date(col("movedIn"), "dd-MM-yyyy") and col("movedOut") === "Null"))
    val updatedRecords_2 = updatesWithDatesBetweenMovedDates.select(col("newId").as("Id"), col("newFirstName").as("firstName"),
      col("newLastName").as("lastName"), col("newAddress").as("address"), col("newMovedIn"),col("movedIn"),
      col("movedOut"), col("status"))
      .withColumn("movedIn", col("newMovedIn"))
      .withColumn("movedOut", col("movedOut"))
      .withColumn("status", col("status"))
      .drop(col("newMovedIn"))
    val updatedHistory_2 = updatesWithDatesBetweenMovedDates.select(col("Id"), col("firstName"), col("lastName"),
      col("address"), col("movedIn"), col("newMovedIn"), col("movedOut"), col("status"))
      .withColumn("movedOut", col("newMovedIn"))
      .withColumn("status", lit(false))
      .drop(col("newMovedIn"))
    val bareHistory_2 = touchedHistory.join(updatedHistory_2, touchedHistory.col("Id") === updatedHistory_2.col("Id"), "left_anti")
    val touchedHistory_2 = updatedHistory_2.union(bareHistory_2).union(updatedRecords_2)
    touchedHistory_2
  }
}



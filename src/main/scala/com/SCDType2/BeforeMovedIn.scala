package com.SCDType2

import org.apache.spark.sql.functions.{col, lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BeforeMovedIn {

  def beforeMovedIn(joinedDataFrame: DataFrame, touchedHistory_2: DataFrame, spark: SparkSession): DataFrame = {

    val updatesWithDatesBeforeMovedIn = joinedDataFrame
      .where(col("newAddress") =!= col("address"))
      .where(to_date(col("newMovedIn"), "dd-MM-yyyy") < to_date(col("movedIn"), "dd-MM-yyyy"))
    val updatedRecords_3 = updatesWithDatesBeforeMovedIn.select(col("newId").as("id"), col("newFirstName").as("firstName"),
      col("newLastName").as("lastName"), col("newAddress").as("address"), col("newMovedIn"),
      col("movedIn"), col("movedOut"), col("status"))
      .withColumn("movedOut", col("movedIn"))
      .withColumn("movedIn", col("newMovedIn"))
      .withColumn("status", lit(false))
      .drop(col("newMovedIn"))
    val updatedHistory_3 = updatesWithDatesBeforeMovedIn.select(col("Id"), col("firstName"), col("lastName"),
      col("address"), col("movedIn"), col("movedOut"), col("status"))
    val bareHistory_3 = touchedHistory_2.join(updatedHistory_3, touchedHistory_2.col("Id") === updatedHistory_3.col("Id"), "left_anti")
    val touchedHistory_3 = updatedHistory_3.union(bareHistory_3).union(updatedRecords_3)
    touchedHistory_3
  }
}

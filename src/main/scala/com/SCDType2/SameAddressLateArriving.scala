package com.SCDType2

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SameAddressLateArriving {

  def sameAddressLateArriving(historyDataframe: DataFrame, updatesDataframe: DataFrame, spark: SparkSession): DataFrame = {
    val lateArrivingSameIdAddressDataframe = historyDataframe.join(updatesDataframe, historyDataframe.col("Id") === updatesDataframe.col("newId"), "inner")
      .where(col("newAddress") === col("Address"))
      .where(to_date(col("newMovedIn"), "dd-MM-yyyy") <= to_date(col("movedIn"),"dd-MM-yyyy"))
    val updatedHistory_1 = lateArrivingSameIdAddressDataframe.select(col("newId").as("Id"),
      col("newFirstName").as("firstName"), col("newLastName").as("lastName"),
      col("newAddress").as("address"), col("newMovedIn").as("movedIn"),
      col("movedOut"), col("status"))
      .withColumn("movedOut", col("movedOut"))
      .withColumn("status", col("status"))
      .withColumn("movedIn", col("movedIn"))
    val bareHistory=historyDataframe.join(updatedHistory_1,historyDataframe.col("id")===updatedHistory_1.col("Id"), "left_anti")
    val result=bareHistory.union(updatedHistory_1)
    result
  }
}

package com.SCDType2

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Duplicates {

  def duplicates(updatesDataframe: DataFrame, spark: SparkSession): DataFrame = {

    val duplicatedDataframe = updatesDataframe.select("*")
      .groupBy("newId","newAddress").agg(min("newMovedIn").as("newMovedIn"))
    val unduplicatedJoinedDataFrame = updatesDataframe.join(duplicatedDataframe, Seq("newId", "newAddress", "newMovedIn"), "inner")
    val unduplicatedDataFrame =unduplicatedJoinedDataFrame.select("newId","newFirstName", "newLastName", "newAddress", "newMovedIn")
    unduplicatedDataFrame
  }
}

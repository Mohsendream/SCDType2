package com.SCDType2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.SCDType2.Duplicates.duplicates
import com.SCDType2.DifferentId.differentId
import com.SCDType2.AfterMovedOut.afterMovedOut
import com.SCDType2.BetweenMovingDates.betweenMovingDates
import com.SCDType2.BeforeMovedIn.beforeMovedIn

object AddressHistoryBuilder {

  def addressHistoryBuilder(historyDataframe: DataFrame, updatesDataframe: DataFrame, spark: SparkSession): DataFrame = {

    val unduplicateUpdates = duplicates(updatesDataframe, spark)
    val newHistory = differentId(historyDataframe, unduplicateUpdates, spark)
    val joinedDataFrame = newHistory.join(unduplicateUpdates, newHistory.col("Id") === unduplicateUpdates.col("newId"), "inner")
    val afterMovedOutHistory = afterMovedOut(joinedDataFrame,newHistory, spark)
    val betweenMovingDatesHistory = betweenMovingDates(joinedDataFrame, afterMovedOutHistory, spark)
    val result = beforeMovedIn(joinedDataFrame, betweenMovingDatesHistory, spark)
    result
  }
}

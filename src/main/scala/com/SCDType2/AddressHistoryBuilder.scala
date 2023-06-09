package com.SCDType2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.SCDType2.Duplicates.duplicates
import com.SCDType2.DifferentId.differentId
import com.SCDType2.AfterMovedOut.afterMovedOut
import com.SCDType2.BetweenMovingDates.betweenMovingDates
import com.SCDType2.BeforeMovedIn.beforeMovedIn
import com.SCDType2.SameAddressBetweenDates.sameAddressBetweenDates
import com.SCDType2.SameAddressLateArriving.sameAddressLateArriving
import com.SCDType2.SameAddressAfterDates.sameAddressAfterDates

object AddressHistoryBuilder {

  def addressHistoryBuilder(historyDataframe: DataFrame, updatesDataframe: DataFrame, spark: SparkSession): DataFrame = {

    val unduplicateUpdates = duplicates(updatesDataframe, spark)
    val newUpdates = sameAddressBetweenDates(historyDataframe, unduplicateUpdates, spark)
    val newHistory = differentId(historyDataframe, unduplicateUpdates, spark)
    val joinedDataFrame = newHistory.join(newUpdates, newHistory.col("Id") === newUpdates.col("newId"), "inner")
    val afterMovedOutHistory = afterMovedOut(joinedDataFrame, newHistory, spark)
    val betweenMovingDatesHistory = betweenMovingDates(joinedDataFrame, afterMovedOutHistory, spark)
    val beforeMovingDatesHistory = beforeMovedIn(joinedDataFrame, betweenMovingDatesHistory, spark)
    val sameAddressLateArrivingHistory = sameAddressLateArriving(joinedDataFrame, beforeMovingDatesHistory, spark)
    val result = sameAddressAfterDates(joinedDataFrame, sameAddressLateArrivingHistory, spark)
    result
  }
}

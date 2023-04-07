package com.SCDType2

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.SCDType2.Duplicates.duplicates
import com.SCDType2.DifferentId.differentId
import com.SCDType2.AfterMovedOut.afterMovedOut
import com.SCDType2.BetweenMovingDates.betweenMovingDates
import com.SCDType2.BeforeMovedIn.beforeMovedIn
import com.SCDType2.SameAddressLateArriving.sameAddressLateArriving
import com.SCDType2.SameAddressBetweenDates.sameAddressBetweenDates

object AddressHistoryBuilder {

  def addressHistoryBuilder(historyDataframe: DataFrame, updatesDataframe: DataFrame, spark: SparkSession): DataFrame = {

    val unduplicateUpdates = duplicates(updatesDataframe, spark)
    val newHistory = differentId(historyDataframe, unduplicateUpdates, spark)
    val newUpdates = sameAddressBetweenDates(newHistory, unduplicateUpdates, spark)
    val lateArrivingSameAddress = sameAddressLateArriving(newHistory, newUpdates, spark)
    val afterMovedOutHistory = afterMovedOut(lateArrivingSameAddress, newUpdates, spark)
    val betweenMovingDatesHistory = betweenMovingDates(afterMovedOutHistory, newUpdates, spark)
    val beforeMovedInHistory = beforeMovedIn(betweenMovingDatesHistory, newUpdates, spark)
    beforeMovedInHistory
  }
}

package com.regit.meetinganalysis.execution

import com.regit.datareader.FileReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

object AnalyzeMeeting {

  def findDailyActiveUsers(sparkSession: SparkSession): Unit={

    print("Inside Find DAU")
    var activityDf=FileReader.readActivityData(sparkSession);

    var userDf=FileReader.readUserData(sparkSession)

    activityDf.show(100);

    var dau=activityDf.join(userDf,"user_id")
      .where("account_id is not null and activity_type='join_meeting'")
      .groupBy("timestamp")
      .count()
      .withColumn("CountOfDailyUsers",col("count"))
      .select("timestamp","CountOfDailyUsers")

    dau.show(100);

  }
}

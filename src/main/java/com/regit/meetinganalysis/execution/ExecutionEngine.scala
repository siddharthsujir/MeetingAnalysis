package com.regit.meetinganalysis.execution

import org.apache.spark.sql.SparkSession
import com.regit.meetinganalysis.execution.AnalyzeMeeting

object ExecutionEngine extends App {

  override def main(args: Array[String]): Unit={
    var sparkSession = SparkSession.builder().appName("MeetingAnalysis")
      .master("local[*]")
      .getOrCreate();


    AnalyzeMeeting.findDailyActiveUsers(sparkSession);
  }
}

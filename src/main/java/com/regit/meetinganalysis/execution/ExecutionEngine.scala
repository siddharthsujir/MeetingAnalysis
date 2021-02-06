package com.regit.meetinganalysis.execution

import org.apache.spark.sql.SparkSession

object ExecutionEngine {

  def main(args: String): Unit={
    var sparkSession = SparkSession.builder().appName("MeetingAnalysis")
      .master("local[*]")
      .getOrCreate();


  }
}

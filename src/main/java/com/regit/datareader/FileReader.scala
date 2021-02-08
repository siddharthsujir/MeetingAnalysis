package com.regit.datareader

import com.regit.caseclass.{Activity, User}
import org.apache.spark.sql.{Dataset, SparkSession}

object FileReader {

  def readUserData(sparkSession: SparkSession): Dataset[User]={

    import sparkSession.implicits._;
    return sparkSession.read
      .option("headers","true")
        .format("csv")
        .load("C:\\Users\\siddhu\\Documents\\zoom_dataset\\user.csv")
        .as[User]
  }

  def readActivityData(sparkSession: SparkSession): Dataset[Activity]={

    import sparkSession.implicits._
    return sparkSession.read
        .option("headers","true")
        .format("csv")
        .load("C:\\Users\\siddhu\\Documents\\zoom_dataset\\activity.csv")
        .as[Activity]
  }
}

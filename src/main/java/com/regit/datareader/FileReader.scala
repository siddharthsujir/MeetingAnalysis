package com.regit.datareader

import com.regit.caseclass.{Activity, User}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

object FileReader {

  def readUserData(sparkSession: SparkSession): Dataset[User]={

    val userSchema= StructType(Array(
      StructField("user_id",IntegerType,true),
      StructField("user_email",StringType,true),
      StructField("account_id",IntegerType,true)
    )
    )

    import sparkSession.implicits._;
    return sparkSession.read
      .option("headers","true")
        .format("csv")
        .schema(userSchema)
        .load("C:\\Users\\siddhu\\Documents\\zoom_dataset\\user.csv")
        .as[User]
  }

  def readActivityData(sparkSession: SparkSession): Dataset[Activity]={

    val schema1= StructType(Array(
       StructField("meeting_id",IntegerType,true),
      StructField("user_id",IntegerType,true),
      StructField("activity_type",StringType,true),
      StructField("timestamp",StringType,true)
    )
    )
    import sparkSession.implicits._
    return sparkSession.read
        .option("headers","true")
        .format("csv")
        .schema(schema1)
        .load("C:\\Users\\siddhu\\Documents\\zoom_dataset\\activity.csv")
        .as[Activity]
        .filter("meeting_id is not null")
  }
}

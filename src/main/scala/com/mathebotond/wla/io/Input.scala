package com.mathebotond.wla.io

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Input(spark: SparkSession) {

  def readCsv(location: String): DataFrame = {
    val old_columns = Seq("_c0", "_c2", "_c11")
    val new_columns = Seq("timestamp", "ip", "endpoint")
    val columnsList = old_columns.zip(new_columns).map(f => {
      col(f._1).as(f._2)
    })

    spark
      .read
      .option("sep", " ")
      .csv(location)
      .select(columnsList: _*)
  }

  def cleanUrl: UserDefinedFunction = udf((request: String) => {
    val noMethod = request.split(" ")(1)
    val noBase = noMethod.replaceAll("https://paytm.com:443", "")
    val noQuery = noBase.split("\\?")(0)
    noQuery
  })

  def parse(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn("timestamp", unix_timestamp(to_timestamp(col("timestamp"))))
      .withColumn("ip", split(col("ip"), ":").getItem(0))
      .withColumn("endpoint", cleanUrl(col("endpoint")))
  }
}

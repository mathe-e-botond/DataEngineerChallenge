package com.mathebotond.wla

import com.mathebotond.wla.io.Input
import org.apache.spark.sql.SparkSession

object LogAnalytics extends SparkJob {
  override def appName: String = "Log Analytics"

  override def run(spark: SparkSession, config: UsageConfig, input: Input): Unit = {
    val logs = input.readAndParse(config.input);



    logs.show(5, truncate = false)
  }
}

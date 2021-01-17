package com.mathebotond.wla

import com.mathebotond.wla.io.{Input, Output}
import org.apache.spark.sql.SparkSession

trait SparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()

    parseAndRun(spark, args)

    def parseAndRun(spark: SparkSession, args: Array[String]): Unit = {
      new UsageOptionParser().parse(args, UsageConfig()) match {
        case Some(config) => run(spark, config, new Input(spark), new Output("result"))
        case None => throw new IllegalArgumentException("arguments provided to job are not valid")
      }
    }
  }

  def run(spark: SparkSession, config: UsageConfig, input: Input, output: Output)

  def appName: String
}

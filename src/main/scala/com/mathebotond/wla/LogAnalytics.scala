package com.mathebotond.wla

import com.mathebotond.wla.io.{Input, Output}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object LogAnalytics extends SparkJob {
  val SESSION_TIMOUT: Int = 15 * 60;

  override def appName: String = "Log Analytics"

  override def run(spark: SparkSession, config: UsageConfig, input: Input, output: Output): Unit = {
    val logs = input.parse(
      input.readCsv(config.input)
    )

    val sessions = extractSessions(logs)

    analyze(sessions, output)
  }

  def isNewSession(current: Column, previous: Column): Column = {
    previous.isNull || current - previous > SESSION_TIMOUT
  }

  def extractSessions(logs: DataFrame): DataFrame = {
    val overColumns = Window.partitionBy("ip").orderBy("timestamp")

    val sessionBeginMarks = logs
      .withColumn("session",
        when(isNewSession(col("timestamp"), lag(col("timestamp"), 1).over(overColumns)),
          concat(col("ip"), lit(" "), col("timestamp"))))

    val allSessionMarked = sessionBeginMarks
      .withColumn("session",
        when(col("session").isNull, last("session", true).over(overColumns)).otherwise(col("session")))

    val session = allSessionMarked.groupBy("session").agg(
      first(col("ip")) as "ip",
      min(col("timestamp")) as "start", // can use first?
      max(col("timestamp")) as "end", // can use last?
      countDistinct("endpoint") as "uniqueUrls"
    )

    session
      .withColumn("duration", col("end") - col("start"))
  }

  def analyze(sessions: DataFrame, output: Output): Unit = {
    output.save("sessions", sessions, compress = true)
    output.save("average-duration", sessions.select(mean(sessions("duration"))))
    output.save("most-engaged", sessions.groupBy("ip").agg(
      sum(col("duration")) as "totalDuration"
    ).orderBy(col("totalDuration").desc).limit(10))
  }
}

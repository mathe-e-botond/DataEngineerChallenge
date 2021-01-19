package com.mathebotond.wla

import com.mathebotond.wla.test.CaptureOutput
import com.mathebotond.wla.test.SharedSparkSession.spark
import org.scalatest.flatspec.AnyFlatSpec

class LogAnalyticsTest extends AnyFlatSpec {
  "extractSessions" should "sessionise logs" in {

    val inputData = spark.createDataFrame(Seq(
      (1437555628, "123.242.248.130", "/shop/authresponse"), //123.242.248.130 session 1 start
      (1437555700, "203.91.211.44", "/shop/wallet/txnhistory"),
      (1437555700, "123.242.248.130", "/papi/v1/expresscart/verify"),
      (1437555723, "123.242.248.130", "/shop/wallet/txnhistory"), //123.242.248.130 session 1 end
      (1437555750, "1.39.32.179", "/papi/v1/expresscart/verify"),
      (1437555830, "203.91.211.44", "/shop/wallet/txnhistory"),
      (1611070910, "123.242.248.130", "/papi/v1/expresscart/verify"), //123.242.248.130 session 2 start
      (1611071010, "123.242.248.130", "/papi/v1/expresscart/verify") //123.242.248.130 session 2 end
    )).toDF("timestamp", "ip", "endpoint")

    val expected = spark.createDataFrame(Seq(
      ("123.242.248.130 1437555628", "123.242.248.130", 1437555628, 1437555723, 3, 95),
      ("203.91.211.44 1437555700", "203.91.211.44", 1437555700, 1437555830, 1, 130),
      ("1.39.32.179 1437555750", "1.39.32.179", 1437555750, 1437555750, 1, 0),
      ("123.242.248.130 1611070910", "123.242.248.130", 1611070910, 1611071010, 1, 100)
    )).toDF("session", "ip", "start", "end", "uniqueUrls", "duration")

    val actual = LogAnalytics.extractSessions(inputData)

    actual.except(expected).show(truncate = false)
    assert(actual.except(expected).isEmpty)
  }


  "analyze" should "calculate metrics from sessions" in {
    import spark.implicits._

    val output = new CaptureOutput("dummy", saveToFile = false)

    val input = spark.createDataFrame(Seq(
      ("123.242.248.130 1437555628", "123.242.248.130", 1437555628, 1437555723, 3, 95),
      ("203.91.211.44 1437555700", "203.91.211.44", 1437555700, 1437555830, 1, 130),
      ("1.39.32.179 1437555750", "1.39.32.179", 1437555750, 1437555750, 1, 0),
      ("123.242.248.130 1611070910", "123.242.248.130", 1611070910, 1611071010, 1, 100)
    )).toDF("session", "ip", "start", "end", "uniqueUrls", "duration")

    val expectedAverageDuration = spark.sparkContext.parallelize(List(
      81.25
    )).toDF("duration")

    val expectedMostEngaged = spark.createDataFrame(Seq(
      ("123.242.248.130", 195, 2),
      ("203.91.211.44", 130, 1),
      ("1.39.32.179", 0, 1),
    )).toDF("ip", "totalDuration", "numberOfSessions")

    LogAnalytics.analyze(input, output)

    val actual = output.results
    assert(actual("average-duration").except(expectedAverageDuration).isEmpty)
    assert(actual("most-engaged").except(expectedMostEngaged).isEmpty)
  }
}

package com.mathebotond.wla

import com.mathebotond.wla.test.SharedSparkSession.spark
import com.mathebotond.wla.io.{Input, Output}
import com.mathebotond.wla.test.CaptureOutput
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec

class LogAnalyticsTest extends AnyFlatSpec {
  "log analytics" should "parse logs" in {
    val config = UsageConfig("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")
    val output = new CaptureOutput("result");
    LogAnalytics.run(spark, config, new Input(spark), output)

    assert(output.results.keySet.equals(Set("sessions", "average-duration", "most-engaged")))
  }
}

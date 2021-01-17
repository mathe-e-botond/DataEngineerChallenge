package com.mathebotond.wla;

import SharedSparkSession.spark
import com.mathebotond.wla.io.{Input, Output}
import org.scalatest.flatspec.AnyFlatSpec

class LogAnalyticsTest extends AnyFlatSpec {
  "log analytics" should "parse logs" in {
    val config = UsageConfig("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")
    LogAnalytics.run(spark, config, new Input(spark), new Output("result"))
  }
}

package com.mathebotond.wla.io

import com.mathebotond.wla.test.SharedSparkSession.spark
import org.scalatest.flatspec.AnyFlatSpec

class InputTest extends AnyFlatSpec {

  "parse" should "clean up messy data" in {
    val input = new Input(spark)

    val inputData = spark.createDataFrame(Seq(
      ("2015-07-22T09:00:28.019143Z", "123.242.248.130:54635", "GET https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0 HTTP/1.1"),
      ("2015-07-22T09:00:27.894580Z", "203.91.211.44:51402", "GET https://paytm.com:443/shop/wallet/txnhistory?page_size=10&page_number=0&channel=web&version=2 HTTP/1.1"),
      ("2015-07-22T09:00:27.885745Z", "1.39.32.179:56419", "POST https://paytm.com:443/papi/v1/expresscart/verify HTTP/1.1")
    )).toDF("timestamp", "ip", "endpoint")

    val expected = spark.createDataFrame(Seq(
      (1437555628, "123.242.248.130", "/shop/authresponse"),
      (1437555627, "203.91.211.44", "/shop/wallet/txnhistory"),
      (1437555627, "1.39.32.179", "/papi/v1/expresscart/verify")
    )).toDF("timestamp", "ip", "endpoint")

    val actual = input.parse(inputData)

    actual.except(expected).show
    assert(actual.except(expected).isEmpty)
  }
}

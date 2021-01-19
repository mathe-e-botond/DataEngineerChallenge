package com.mathebotond.wla.test

import com.mathebotond.wla.io.Output
import org.apache.spark.sql.DataFrame

class CaptureOutput(path: String, saveToFile: Boolean = true) extends Output(path) {
  var results: Map[String, DataFrame] = Map();

  override def save(name: String, dataFrame: DataFrame, compress: Boolean): Unit = {
    if (saveToFile) {
      super.save(name, dataFrame, compress)
    }
    results += (name -> dataFrame)
  }
}

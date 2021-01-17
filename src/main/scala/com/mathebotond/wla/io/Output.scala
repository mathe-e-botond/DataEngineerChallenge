package com.mathebotond.wla.io

import org.apache.spark.sql.{DataFrame, SaveMode}

class Output(path: String) {
  def save(name: String, dataFrame: DataFrame, compress: Boolean = false) = {
    var saveSettings = dataFrame
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)

    if (compress) {
      saveSettings = saveSettings.option("compression", "gzip")
    }

    saveSettings.save(path + "/" + name)
  }
}

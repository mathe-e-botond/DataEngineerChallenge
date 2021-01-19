package com.mathebotond.wla.io

import org.apache.spark.sql.{DataFrame, SaveMode}

class Output(path: String) {
  def save(name: String, dataFrame: DataFrame, compress: Boolean = false) = {
    var saveSettings = dataFrame
      .coalesce(1)
      .write
      .format("csv")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
      .option("header", "true")
      .mode(SaveMode.Overwrite)

    if (compress) {
      saveSettings = saveSettings.option("compression", "gzip")
    }

    saveSettings.save(path + "/" + name)
  }
}

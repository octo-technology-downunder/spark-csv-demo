package com.octo.spark.csv

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

object ExportJsonAsCsv {

  val JSON_FILE = "src/main/resources/kiosks.json"
  val CSV_DEST_DIR = "/tmp/resuts/"

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
                      .setAppName("ExportJsonAsCsv")
                      .setMaster("local[*]")
    val sqlContext = new SQLContext(new SparkContext(sparkConf))

    val dataFrame = sqlContext.load("org.apache.spark.sql.json", Map("path" -> JSON_FILE))
    dataFrame.save("com.databricks.spark.csv", SaveMode.ErrorIfExists, Map("path" -> CSV_DEST_DIR, "header" -> "true"))
  }

}

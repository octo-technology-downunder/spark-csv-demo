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
    val dataFrame = sqlContext
      .read
      .options(Map("path" -> JSON_FILE))
      .format("org.apache.spark.sql.json")
      .load()

    dataFrame
      .write
      .format("com.databricks.spark.csv")
      .mode(SaveMode.ErrorIfExists)
      .options(Map("path" -> CSV_DEST_DIR, "header" -> "true"))
      .save()
  }

}

package com.octo.spark.csv

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LoadCsvAsDataframe {

  val CSV_FILE = "src/main/resources/kiosks.csv"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
                      .setAppName("LoadCsvAsDataframe")
                      .setMaster("local[*]")

    val sqlContext = new SQLContext(new SparkContext(sparkConf))
    val dataFrame = sqlContext
      .read
      .options(Map("path" -> CSV_FILE, "header" -> "true"))
      .format("com.databricks.spark.csv")
      .load()

    dataFrame.printSchema()
    dataFrame.registerTempTable("kiosksErrors")

    val results = sqlContext.sql("select sum(numberOfErrors) from kiosksErrors")
    println(results.collectAsList())
  }

}

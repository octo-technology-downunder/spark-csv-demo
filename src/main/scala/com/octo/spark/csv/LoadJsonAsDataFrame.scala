package com.octo.spark.csv

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LoadJsonAsDataFrame {

  val JSON_FILE = "src/main/resources/kiosks.json"

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
                          .setAppName("LoadJsonAsDataFrame")
                          .setMaster("local[*]")
    val sqlContext = new SQLContext(new SparkContext(sparkConf))

    val dataFrame = sqlContext.load("org.apache.spark.sql.json", Map("path" -> JSON_FILE))
    dataFrame.printSchema()
    dataFrame.registerTempTable("kiosksErrors")

    val results = sqlContext.sql("select sum(numberOfErrors) from kiosksErrors")
    println(results.collectAsList())
  }

}

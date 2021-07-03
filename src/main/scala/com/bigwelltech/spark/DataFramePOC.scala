package com.bigwelltech.spark

import org.apache.spark.sql.SparkSession
import com.bigwelltech.spark.util.SparkInitializer

object DataFramePOC extends SparkInitializer {
  def main(args: Array[String]): Unit = {
  // val dataframe=sparkSession.read.option("header", true).csv("src/main/resources/demo.csv")
   val dataframe=sparkSession.read.option("header", true).format("CSV").load("src/main/resources/demo.csv")
   dataframe.show()
  }
}
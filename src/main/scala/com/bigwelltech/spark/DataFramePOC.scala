package com.bigwelltech.spark

import org.apache.spark.sql.SparkSession
import com.bigwelltech.spark.util.SparkInitializer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object DataFramePOC extends SparkInitializer {

  def readCSVFile(path: String) {
    // val dataframe=sparkSession.read.option("header", true).csv("src/main/resources/demo.csv")
    val schama = StructType(Array(
      StructField("Name", StringType, true),
      StructField("Company", StringType, true),
      StructField("City", StringType, true),
      StructField("Country", StringType, true),
      StructField("Salary", StringType, true),
      StructField("Phone", StringType, true)))
    val dataframe = sparkSession.read.option("header", true).option("delimiter", "#").schema(schama)
      .format("CSV").load(path)
    dataframe.show()
  }
  def main(args: Array[String]): Unit = {
    readCSVFile("src/main/resources/demo.csv")
  }
}
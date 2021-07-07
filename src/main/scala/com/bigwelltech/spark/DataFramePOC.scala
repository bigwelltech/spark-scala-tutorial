package com.bigwelltech.spark

import org.apache.spark.sql.SparkSession
import com.bigwelltech.spark.util.SparkInitializer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object DataFramePOC extends SparkInitializer {
  def main(args: Array[String]): Unit = {
    // val dataframe=sparkSession.read.option("header", true).csv("src/main/resources/demo.csv")
    val schama = StructType(Array(
      StructField("Name", StringType, true),
      StructField("City", StringType, true),
      StructField("Company", StringType, true),
      
      StructField("Country", StringType, true),
      StructField("Salary", StringType, true),
      StructField("Phone", StringType, true)
      ))
    val dataframe = sparkSession.read.option("header", true).schema(schama).format("CSV").load("src/main/resources/demo.csv")
    dataframe.show()
    
  }
}
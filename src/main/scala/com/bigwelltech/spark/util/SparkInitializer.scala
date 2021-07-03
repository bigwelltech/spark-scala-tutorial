package com.bigwelltech.spark.util

import org.apache.spark.sql.SparkSession

trait SparkInitializer {
   System.setProperty("hadoop.home.dir", "D:/bigwelltech/sparkconf/conf/")
    val sparkSession = SparkSession.builder().appName("Spark-Scala_Tutorial").master("local").getOrCreate()
    
}
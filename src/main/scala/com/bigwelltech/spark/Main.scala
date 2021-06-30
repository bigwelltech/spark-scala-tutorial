package com.bigwelltech.spark

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    println("**Start**")
    System.setProperty("hadoop.home.dir", "D:/bigwelltech/sparkconf/conf/")
    val sparkSession = SparkSession.builder().appName("Spark-Scala_Tutorial").master("local").getOrCreate()
    val rdd = sparkSession.sparkContext.parallelize(Seq("one", "two", "three"))
    rdd.foreach(f => println(f))
    println(rdd.count())
    println("**End**")
  }
}
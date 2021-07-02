package com.bigwelltech.spark

import org.apache.spark.sql.SparkSession

object Main {
  def getRddByCollect(sparkSession: SparkSession) {
    val rdd = sparkSession.sparkContext.parallelize(Seq("one", "two", "three"))
    rdd.foreach(f => println(f))
    println(rdd.count())
  }
  def getRddByTextFile(sparkSession: SparkSession) {
    val rdd = sparkSession.sparkContext.textFile("src/main/resources/test.txt")
    rdd.foreach(f => println(f))
    println(rdd.count())
  }

  def mapAndFlatMap(sparkSession: SparkSession) {
    //map ->one to one
    //flat -> one to many
    val rdd = sparkSession.sparkContext.textFile("src/main/resources/test.txt")
    val maprdd = rdd.map(x => (x, x.length()))
    maprdd.foreach(f => println(f))
    println(maprdd.count())
    println("****FlatMap*****")
    val flatmap=rdd.flatMap(x=>x.split(" "))
    val myrdd=flatmap.map(f=>(f,f.length()))
    myrdd.foreach(x=>println(x))
    println(myrdd.count())

  }

  def main(args: Array[String]): Unit = {
    println("**Start**")
    System.setProperty("hadoop.home.dir", "D:/bigwelltech/sparkconf/conf/")
    val sparkSession = SparkSession.builder().appName("Spark-Scala_Tutorial").master("local").getOrCreate()
    //getRddByCollect(sparkSession)
    //getRddByTextFile(sparkSession)
    mapAndFlatMap(sparkSession)
    println("**End**")
  }
}
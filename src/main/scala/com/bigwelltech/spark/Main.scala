package com.bigwelltech.spark

import org.apache.spark.sql.SparkSession
import com.bigwelltech.spark.util.SparkInitializer

object Main extends SparkInitializer {
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
    val flatmap = rdd.flatMap(x => x.split(" "))
    val myrdd = flatmap.map(f => (f, f.length()))
    myrdd.foreach(x => println(x))
    println(myrdd.count())

  }
  def getWordOccurance(sparkSession: SparkSession) {
    val filerdd = sparkSession.sparkContext.textFile("src/main/resources/test.txt")
    val flatrdd = filerdd.flatMap(x => x.split(" "))
    val maprdd = flatrdd.map(x => (x, 1))
    val rdd = maprdd.reduceByKey(_ + _)
    rdd.foreach(f => println(f))
    println(rdd.count())
  }
  def main(args: Array[String]): Unit = {
    println("**Start**")
    //getRddByCollect(sparkSession)
    //getRddByTextFile(sparkSession)
    //mapAndFlatMap(sparkSession)
    getWordOccurance(sparkSession)
    println("**End**")
  }
}
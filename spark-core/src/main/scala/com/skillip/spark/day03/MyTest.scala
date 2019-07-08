package com.skillip.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object MyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("My Test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(1 to 100)
//    rdd.map{x => println(x); x}.sortBy(x => x)
    rdd.map{x => x}.sortBy{x => x}
  }
}

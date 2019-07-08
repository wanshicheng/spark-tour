package com.skillip.spark.day01

import org.apache.spark.{SparkConf, SparkContext}

object MakeRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MakeRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
    rdd.flatMap(x => Array(x, x * x, x * x * x)).collect.foreach(println)
    sc.stop
  }
}

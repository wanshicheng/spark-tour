package com.skillip.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDTest extends App {
  private val conf: SparkConf = new SparkConf().setAppName("RDD Test").setMaster("local[2]")
  private val sc = new SparkContext(conf)
//  private val rdd1: RDD[Int] = sc.parallelize(1 to 10)
//  rdd1.mapPartitions(it => it).foreach(println)


  val a = sc.parallelize(0 to 9, 3)
  def doubleFunc(iter: Iterator[Int]) : Iterator[(Int,Int)] = {
    var res = List[(Int,Int)]()
    while (iter.hasNext)
    {
      val cur = iter.next
      res .::= (cur,cur*2)
    }
    res.iterator
  }
  val result = a.mapPartitions(doubleFunc)
  println(result.collect().mkString)


  val rdd: RDD[Int] = sc.parallelize(0 to 9)
  val rdd4p: RDD[Int] = sc.parallelize(0 to 9, 4)
  val strRDD: RDD[String] = sc.parallelize(Array("Hello", "World!"))
  val repRdd: RDD[Range.Inclusive] = sc.parallelize(List(0 to 9) ::: List(0 to 9))

  println("----------glom()----------")
  rdd.glom().map(_.toList).foreach(println) //元素个数与分区数相关
  rdd4p.glom().map(_.toList).foreach(println)

  println("----------groupBy(func)----------")
  // 默认情况不会 shuffle
  rdd.groupBy(x => if(x % 2 == 1) "odd" else "even").foreach(println)
  rdd.groupBy(x => "hehe").foreach(println)

  println("----------filter(func)----------")
  rdd.filter(_ % 2 == 0).foreach(println)
  strRDD.filter(_.toLowerCase.contains("hello")).foreach(println)

  println("----------sample(withReplacement, fraction, seed)----------")
  rdd.sample(false, 0.2).foreach(println)
//  rdd.sample(false, 1.2).foreach(println)
  println("----------")
  rdd.sample(true, 1.2).foreach(println)

  println("----------distinct([numTasks]))----------")
  repRdd.distinct(8).foreach(println)

  println("----------mapPartitions(func)----------")


}



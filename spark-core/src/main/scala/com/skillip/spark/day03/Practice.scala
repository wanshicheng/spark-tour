package com.skillip.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

object Practice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:/agent.log")

    val provinceAdsOne = lines.map(line => {
      val split = line.split("\\W+")
      ((split(1), split(4)), 1)
    })
    val provinceAdsSum = provinceAdsOne.reduceByKey(_ + _)
//    provinceAdsSum.map{
//      case ((pid, aid), sum) =>  (pid, (aid, sum))
//    }
    val proviceAdsSumMd = provinceAdsSum.map(ele => (ele._1._1,(ele._1._2,ele._2)))

    val provinceGroupAdsCount = proviceAdsSumMd.groupByKey

    val provinceAdsCountTop3 = provinceGroupAdsCount.mapValues(adsCountIt =>
      adsCountIt.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
    )
    provinceAdsCountTop3.sortBy(_._1.toInt).collect.foreach(println)

    sc.stop
  }
}

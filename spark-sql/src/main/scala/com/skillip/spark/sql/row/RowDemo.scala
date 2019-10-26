package com.skillip.spark.sql.row

import org.apache.spark.sql._

/**
 * 参考资料：http://spark.apache.org/docs/2.4.0/api/scala/index.html#org.apache.spark.sql.Row
 */
object RowDemo {
  def main(args: Array[String]): Unit = {
    val row = Row("1", 2, 3, "test")
    val firstValue = row(0)
    val firstInt = row.getInt(0)
    println(firstValue)
    println(firstInt)
  }
}

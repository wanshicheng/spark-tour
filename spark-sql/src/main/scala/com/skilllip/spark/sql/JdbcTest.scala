package com.skilllip.spark.sql

import org.apache.spark.sql.SparkSession

object JdbcTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("JDBC Demo")
      .getOrCreate()
    import spark.implicits._
    val answerDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.1.113/")
      .option("dbtable", "homework.hwk_answer")
      .option("user", "dev_user")
      .option("password", "Dev!123123")
      .load()

    answerDF.cache()


    val answerBatchDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.1.113/")
      .option("dbtable", "homework.hwk_answer_batch")
      .option("user", "dev_user")
      .option("password", "Dev!123123")
      .load()

    answerBatchDF.cache()
    answerDF.col("id")
    val resultDF = answerDF
      .filter($"batch_no" =!= "null")
      .join(answerBatchDF, Seq("batch_no"), "left")
        .select(answerDF("id") as("aid"), answerBatchDF("id") as("bid"))
      resultDF.show()
    resultDF.write.json("/spark/sql")
  }
}
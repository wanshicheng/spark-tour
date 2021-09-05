package io.github.wanshicheng.hive.jdbc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

import java.util.Properties

object HiveJDBCDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Hive JDBC")
      .getOrCreate()

    JdbcDialects.registerDialect(HiveSqlDialect)

    import spark.implicits._
    val url = "jdbc:hive2://127.0.0.1:10001/default;transportMode=http;httpPath=cliservice"
    val properties = new Properties()
    properties.put("driver", "org.apache.hive.jdbc.HiveDriver")
    val df = spark.read.jdbc(url, "test", properties)
    println(df.queryExecution)

    df.createOrReplaceTempView("student")

    spark.sql("SELECT * FROM student")
      .foreach(println(_))
  }


  case object HiveSqlDialect extends JdbcDialect {
    override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

    override def quoteIdentifier(colName: String): String = {
      colName.substring(colName.indexOf('.') + 1)
    }
  }
}

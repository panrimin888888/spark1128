package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

object JDBCReed {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("JDBCReed")
      .getOrCreate()
    import spark.implicits._
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url",url)
      .option("user",user)
      .option("password","123321")
      .option("dbtable","user")
      .load()
    jdbcDF.show()
  }
}

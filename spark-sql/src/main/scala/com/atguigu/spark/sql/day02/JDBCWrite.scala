package com.atguigu.spark.sql.day02

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JDBCWrite {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("JDBCReed")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.json("e:/people.json")
    val popes = new Properties()
    popes.setProperty("user",user)
    popes.setProperty("password","123321")
    df.write.mode(saveMode= "overwrite" ).jdbc(url,"user_1",popes)
  }
}

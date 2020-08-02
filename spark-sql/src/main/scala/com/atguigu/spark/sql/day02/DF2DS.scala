package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

object DF2DS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DF2DSS").getOrCreate()
    import spark.implicits._
    val df = spark.read.json("e://people.json")
    val ds = df.as[User1]
    ds.show
    val df1 = ds.toDF()
    //df4.show
  }
}
case class User1(name: String,salary: Long)
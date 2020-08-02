package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

object RDD2DF_2 {
  def main(args: Array[String]): Unit = {
//    // 1. 首先需要创建 SparkSession
//    val spark = SparkSession.builder()
//      .master("local[2]")
//      .appName("RDD2DF_2")
//      .getOrCreate()
//    import spark.implicits._
//    val list = User(10,"lisi")::User(20,"ww")::User(15,"zl")::Nil
//    val rdd = spark.sparkContext.parallelize(list)
//    val df = rdd.toDF
//    df.show()
//    spark.close()
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF2")
      .getOrCreate()
    val list = User(10,"a")::User(20,"b")::User(30,"c")::Nil
    val rdd = spark.sparkContext.parallelize(list)
    import spark.implicits._
    val df = rdd.toDF()
    df.show()
    spark.close()
  }
}
//case class User(age: Int,name: String)
case class User(age: Int,name: String)
package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

object RDD2DF {
  def main(args: Array[String]): Unit = {
    // 1. 首先需要创建 SparkSession
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("RDD2DF")
      .getOrCreate()

    // 2. 得到df, 使用df编程
    val df = spark.read.json("e:/people.json")
    df.createOrReplaceTempView("p")
    spark.sql("select * from p").show
    // 把rdd转成df,需要导入:import spark.implicits._
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(List((10,"a"),(20,"b"),(30,"c"),(40,"d")))
    val df1 = rdd.toDF("age","name")
    df1.printSchema()  //获取各个字段信息（数据的结构信息）
    df1.show()
    //3.关闭
    spark.close()
  }
}

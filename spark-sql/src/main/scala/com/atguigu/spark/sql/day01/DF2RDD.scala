package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

object DF2RDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("DF2RDD")
      .getOrCreate()
    val df = spark.read.json("e:/people.json")
    df.printSchema()
    // 从df转成rdd之后, rdd内存储的用意是Row
    // Row当成一个集合, 表示一行数据, 弱类型(类型与jdbc中的ResultSet)
    val rdd = df.rdd
    val rdd2 = rdd.map(row => {
      val name1 = row.getString(0)
      val salary1 = if(row.isNullAt(1)) -1 else row.getLong(1)

      val name = row.get(0)
      val salary = row.get(1)
      (name1,salary1)
    })
    rdd2.collect.foreach(println)

  }
}

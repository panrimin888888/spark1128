package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

object UDFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]").appName("udf").getOrCreate()
    val df = spark.read.json("E://people.json")
    df.createOrReplaceTempView("p")
    //spark.sql("select * from p").show()
    spark.udf.register("toUpper",(s: String) => s.toUpperCase)
    spark.sql("select toUpper(name) name,salary from p").show
  }
}

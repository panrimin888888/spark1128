package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Hello2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Hello2").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val map1 = Map("hello" -> 1,"java" -> 2,"hello" -> 2)

    val rdd = sc.parallelize(map1.toList)
    val rdd1 = rdd.reduceByKey(_+_).collect()

    sc.stop()
  }
}

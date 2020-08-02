package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

object MapValueTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapValueTest")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd2 = rdd1.mapValues("<" + _ + ">")
    rdd2.collect.foreach(println)
    sc.stop()
  }
}

package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

object CogroupTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("CogroupTest")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1,10),(2,20),(3,30),(5,50)))
    val rdd2 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c"),(1,"aa"),(4,"d")))
    val rdd3 = rdd1.cogroup(rdd2)
    rdd3.collect.foreach(println)
    sc.stop()
  }
}

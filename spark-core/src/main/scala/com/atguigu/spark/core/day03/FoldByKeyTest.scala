package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

object FoldByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List("hello" -> 1,"hello" -> 1,"world" -> 1,"spark" -> 1,"hello" -> 1)
    val rdd1 = sc.parallelize(list1,2)
    val rdd2 = rdd1.foldByKey(0)(_+_)
    rdd2.collect.foreach(println)
    sc.stop()
  }
}

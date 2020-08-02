package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SortByKeyTest")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((1,"a"),(1,"b"),(2,"c"),(4,"d")))
    val rdd2 = sc.parallelize(Array((1,"aa"),(3,"bb"),(2,"cc"),(2,"dd")))
    //val rdd3 = rdd1.join(rdd2)
    //val rdd3 = rdd1.leftOuterJoin(rdd2)
    //val rdd3 = rdd1.rightOuterJoin(rdd2)
    val rdd3 = rdd1.fullOuterJoin(rdd2)
    rdd3.collect.foreach(println)
    sc.stop()
  }
}

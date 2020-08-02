package com.atguigu.spark.core.day02

import org.apache.spark.{SparkConf, SparkContext}

object SortByTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List(11,99,30,41,50)
    val list2 = List("aa","aab","abcd","abcc","abc")
    val rdd1 = sc.parallelize(list1)
    //val rdd2 = rdd1.sortBy(x => x,false)
    val rdd2 = sc.parallelize(list2)
    //val rdd4 = rdd3.sortBy(x => (x.length,x),false)
    //rdd4.collect.foreach(println)
    val rdd3 = rdd2.pipe("p1.sh")

    sc.stop()
  }
}

package com.atguigu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object MapPartitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List(10,20,31,40,51,33)
    val rdd1 = sc.parallelize(list1,2)
    //val rdd2 = rdd1.map(_*2)
    //val rdd2 = rdd1.mapPartitions(ele => ele.map(_*2))
    val rdd2 = rdd1.mapPartitionsWithIndex((index,its) => its.map((_,index)))
    //rdd2.collect.foreach(println)
    val rdd3 = rdd1.groupBy(x => x % 2).map{
      case (x,it) => (x,it.toList.sortBy(-_))
    }
    rdd3.collect.foreach(println)
    sc.stop()
  }
}

package com.atguigu.spark.core.day02

import org.apache.spark.{SparkConf, SparkContext}

object DoubleValueTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List(11,20,30,41,50,80)
    val list2 = List(11,20,80,41,50,80,90,70)
    val rdd1 = sc.parallelize(list1,2)
    val rdd2 = sc.parallelize(list2,2)
    //val rdd3 = rdd1.zip(rdd2) //拉链操作
    //val rdd3 = rdd1.zipWithIndex()
    val rdd3 = rdd1.zipPartitions(rdd2)((it1,it2) => it1.zip(it2))
    rdd3.collect.foreach(println)
//    val rdd3 = sc.parallelize(list1.init)
//    val rdd4 = sc.parallelize(list1.tail)
//    val rdd5 = rdd3.zip(rdd4).map({
//      case (k,v) => s"$k -> $v"
//    })
//    rdd5.collect.foreach(println)
    sc.stop()
  }
}

package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}


object SortByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("SortByKeyTest")
    val sc = new SparkContext(conf)
    sc.textFile(args(0))
        .flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
        .map{
          case (k,v) => (v,k)
        }
        .sortByKey(ascending = false)
        .map{
          case (v,k) => (k,v)
        }
        .collect
        .foreach(println)
//    sc
//    .textFile(args(0))
//      .flatMap(_.split(" "))
//      .map((_,1))
//      .reduceByKey(_+_)
//      .sortBy(x=>x._2,false)
//      .collect
//      .foreach(println)
    sc.stop()
  }
}

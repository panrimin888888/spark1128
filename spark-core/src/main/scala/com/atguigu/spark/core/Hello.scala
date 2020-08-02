package com.atguigu.spark.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Hello {
  def main(args: Array[String]): Unit = {
    //1.创建一个sparkcontext
    val conf = new SparkConf().setMaster("local[*]").setAppName("Hello")
    val sc: SparkContext = new SparkContext(conf)
    //2.通过数组源得到数据，第一个RDD
    val lineRDD = sc.parallelize(Seq("a","b","c"))
    //3.对RDD做各种转换
    val wordCount = lineRDD.flatMap(_.split("")).map((_,1)).reduceByKey(_+_)
    //4.执行一个行动算子（collect:把每个executor中执行的结果，收集到driver端）
    val arr = wordCount.collect
    //arr.foreach(println)
    //5.关闭sparkcontext
    sc.stop()

  }
}

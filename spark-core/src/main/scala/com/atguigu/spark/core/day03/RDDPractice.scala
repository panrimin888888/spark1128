package com.atguigu.spark.core.day03

import org.apache.spark.{SparkConf, SparkContext}

object RDDPractice {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("RDDPractice")
    val sc = new SparkContext(conf)
    //读取原始数据
    val lineRDD = sc.textFile(args(0))
//    val rdd1 = lineRDD.map(line => {
//      val split = line.split(" ")
//      ((split(1), split(4)), 1)
//    })
//    //RDD(((pro1,ads1),1),(...)) reducebykey
//    val rdd2 = rdd1.reduceByKey(_+_)
//    //RDD(((pro1,ads1),count),(...)) map
//    val rdd3 = rdd2.map {
//      case ((a, b), count) => (a, (b, count))
//    }
//    //RDD((pro1 -> (ads1,count),...),(... ))  groupbykey
//    val rdd4 = rdd3.groupByKey()
//    //RDD((pro1 -> List(ads1 -> 100,ads2 -> 90,ads3 -> 80,...)),(...))  map 排序
//    val rdd5 = rdd4.map{
//      case (pro,adsCount: Iterable[(String,Int)]) => (pro,adsCount.toList.sortBy(_._2).take(3))
//    }.sortBy(_._1.toInt)
//    //RDD((pro1 -> List(ads1 -> 100,ads2 -> 90,ads3 -> 80)),(pro2 -> List(ads1 -> 100,ads2 -> 90,ads3 -> 80)))
//    rdd5.collect.foreach(println)
//    sc.stop()
//    val rdd1 = lineRDD.map(line => {
//      val words = line.split(" ")
//      ((words(2),words(4)),1)
//    })
//    //RDD((pro1,ads1) -> 1) reducebykey(_+_)
//    val rdd2 = rdd1.reduceByKey(_+_)
//    //RDD((pro1,ads1) -> 100,...) map
//    val rdd3 = rdd2.map({
//      case ((a,b),count) => (a,(b,count))
//    })
//    //RDD((pro1 ->(ads1,100)),(pro1 -> (ads1,90)),....) groupbykey
//    val rdd4 = rdd3.groupByKey()
//    //RDD((pro1 -> List((ads1 -> 100),ads2 -> 90),(ads3 -> 80)...),(...)) map,排序，take(3)
//    val rdd5 = rdd4.map{
//      case (pro,adsCount) => (pro,adsCount.toList.sortBy(-_._2).take(3))}.sortBy(_._1.toInt)
//    //RDD((pro1 -> List((ads1 -> 100),ads2 -> 90),ads3 -> 80),(..))
//    rdd5.collect.foreach(println)
//    sc.stop()
    val rdd1 = lineRDD.map(line => {
      val words = line.split(" ")
      ((words(2),words(4)),1)
    })
    //RDD((pro1,ads1)->1) reducebykey(_+_)
    val rdd2 = rdd1.reduceByKey(_+_)
    //RDD((pro1,ads1)->..,...) map
    val rdd3 = rdd2.map{
      case ((p,a),n) => (p,(a,n))
    }
    //RDD(pro1->(ads1->..),pro2->(ads2->..)..) groupbykey
    val rdd4 = rdd3.groupByKey()
    //RDD((pro1->List(ads1->..,ads2->..,..adsn->...)),(...)) map 排序，take(3)
    val rdd5 = rdd4.map({
      case (pro,it: Iterable[(String,Int)]) =>
        (pro,it.toList.sortBy(-_._2).take(3))})
      .sortBy(_._1.toInt)
    //RDD((pro1->(List(ads1->100,ads2->90,ads3->80))),(...))
    rdd5.collect.foreach(println)
    sc.stop()
  }
}

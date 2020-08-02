package com.atguigu.spark.core.day02
import org.apache.spark.{SparkConf, SparkContext}
object DistinctTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List(11,20,30,41,50,50,50,50)
    val rdd1 = sc.parallelize(list1)
    val rdd2 = rdd1.distinct()
    rdd2.collect.foreach(println)
    sc.stop()
  }
}

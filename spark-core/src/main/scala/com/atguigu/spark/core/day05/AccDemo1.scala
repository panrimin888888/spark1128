package com.atguigu.spark.core.day05
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AccDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    val rdd1: RDD[Int] = sc.parallelize(list1, 2)
    // 累加器 : 系统提供的累加器
    val acc = sc.longAccumulator
    rdd1.foreach(x => {
      acc.add(x)
    })
    println(acc.value)
    sc.stop()
  }
}

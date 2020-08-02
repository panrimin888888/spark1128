package com.atguigu.spark.core.day02
import org.apache.spark.{SparkConf, SparkContext}
object FlatMapTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List(1,2,3,4,5,5,0)
    val rdd1 = sc.parallelize(list1)
    //val rdd2 = rdd1.flatMap(x => Array(2*x,3*x))
    //val rdd2 = rdd1.groupBy(x => if(x % 2 == 1) "old" else "new")
    //val rdd2 = rdd1.flatMap(x => Array(x*x,x*x*x))
    //val rdd2 = rdd1.flatMap(x => Array(x*x,x*x*x))
    //val rdd2 = rdd1.groupBy(x => if(x % 2 == 0) "偶数" else  "奇数")
    //val rdd2 = rdd1.glom()
    //val rdd2 = rdd1.filter(_>3)
    //val rdd2 = rdd1.sample(true,0.8,1)
    //val rdd2 = rdd1.distinct()
    val rdd2 = rdd1.sortBy(x=>x,false)
    rdd2.collect.foreach(println)

    sc.stop()
  }
}

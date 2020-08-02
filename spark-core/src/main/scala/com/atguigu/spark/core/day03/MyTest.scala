package com.atguigu.spark.core.day03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object MyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MyTest")
    val sc = new SparkContext(conf)
    val list1 = List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))
    val rdd1 = sc.parallelize(list1,3)
    val rdd3 = rdd1.map(x => (x._1,x._2+1))
    //rdd3.collect.foreach(println)
    val string = rdd3.toDebugString
    println(string)
    //val rdd2 = rdd1.foldByKey(0)(_+_)
//    val rdd2 = rdd1.aggregateByKey(Int.MinValue)(
//      (u,v) => u.max(v),
//      (max1,max2) => max1+max2
//    )
//    val rdd2 = rdd1.aggregateByKey((Int.MinValue,Int.MaxValue))(
//      {
//        case ((mx,mn),v) => (mx.max(v),mn.min(v))
//      },
//      {
//        case ((max1,min1),(max2,min2)) => (max1+max2,min1+min2)
//      }
//    )
    //分区内，和，个数
    val rdd2 = rdd1.aggregateByKey((0,0))(
      {
        case ((s,c),v) => (s+v,c+1)
      },
      {
        case ((sum1,count1),(sum2,count2)) => (sum1+sum2,count1+count2)
      }
    ).mapValues{
  case (a,b) => a.toDouble / b
}
    //rdd2.collect.foreach(println)
    sc.stop()
  }
}

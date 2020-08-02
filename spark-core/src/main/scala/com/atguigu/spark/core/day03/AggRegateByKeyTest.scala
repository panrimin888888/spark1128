package com.atguigu.spark.core.day03

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object AggRegateByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapPartitions")
    val sc = new SparkContext(conf)
    val list1 = List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))
    val rdd1 = sc.parallelize(list1,2)
    //求每个分区key的最大值,最后求和
//    val rdd2 = rdd1.aggregateByKey(Int.MinValue)(
//      (u,v) => u.max(v),
//      (max1,max2) => max1+max2)
    //求每个分区key的最大值和最小值，最大值求和，最小值求和
//    val rdd2 = rdd1.aggregateByKey((Int.MinValue,Int.MaxValue))(
//      {
//        case ((max,min),value) => (max.max(value),min.min(value))
//      },
//    {
//      case ((max1,min1),(max2,min2)) => (max1+max2,min1+min2)
//    })
    //求每个key的平均值
    //分区内是：和，个数 ； 分区外：总和，总个数
//    val rdd2 = rdd1.aggregateByKey((0,0))(
//      {
//        case ((sum,count),value) => (sum+value,count+1)
//      },
//      {
//        case ((sum1,count1),(sum2,count2)) => (sum1+sum2,count1+count2)
//      }
//    ).mapValues{
//  case (a,b) => a.toDouble / b
//}
    val rdd2 = rdd1.aggregateByKey((0,0))(
      {
        case ((u,v),value) => (u+value,v+1)
      },
      {
        case ((sum1,count1),(sum2,count2)) => (sum1+sum2,count1+count2)
      }
    ).mapValues{
  case (k,v) => k.toDouble / v
}
    rdd2.collect.foreach(println)
    sc.stop()
  }
}

package com.atguigu.spark.core.day04

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Mypartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MyPartitioner")
    val sc = new SparkContext(conf)
    val list1 = List(30, 50, 7, 60, 1, 20, null, null)
    val rdd1 = sc.parallelize(list1,4).map((_,1))
    val rdd2 = rdd1.partitionBy(new MypartitionerDemo(2))
    val rdd3 = rdd2.reduceByKey(new MypartitionerDemo(2),_+_)
    rdd3.glom().map(_.toList).collect.foreach(println)
    Thread.sleep(100000000)
    sc.stop()
  }
}
class MypartitionerDemo(val num: Int) extends Partitioner{
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    key match {
      case null => 0
      case _ => key.hashCode().abs % num
    }
  }

  override def hashCode(): Int = num

  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case o: MypartitionerDemo => num == o.num
      case _ => false
    }
  }
}
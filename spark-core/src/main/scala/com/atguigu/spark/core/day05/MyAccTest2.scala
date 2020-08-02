package com.atguigu.spark.core.day05

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object MyAccTest2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Acc2").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val list1 = List(30, 50, 70, 60, 10, 20)
    // 一次计算出来 元素的和, 个数, 平均值, 最大值, 最小值
    // Map("sum" -> .., "avg"-> ...)
    val rdd1 = sc.parallelize(list1, 3)

    val acc = new MyAcc
    sc.register(acc,"mapAcc")
    rdd1.foreach(x => acc.add(x))
    val map: Map[String, Double] = acc.value
    println(map)
    sc.stop()

  }
}

class MyAcc extends AccumulatorV2[Int,Map[String,Double]]{
  // 仅仅内部使用
  private var map = Map[String,Double]()
  // 判零. 对缓冲区进行初始化判断
  override def isZero: Boolean = map.isEmpty // 集合是空, 就应该返回true
  // 复制累加器. 返回一个新的累加器, 让当前累加器中缓冲区的值, 复制到新的累加器
  override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
    val acc = new MyAcc
    acc.map = this.map
    acc
  }
  // 重置累加器. 把累加器中缓冲区的值重置为 "零"
  override def reset(): Unit = {
    map = Map.empty[String,Double] // 等价于: map = Map[String, Double]()
  }
  // 累加. 分区内累加
  override def add(v: Int): Unit = {
    // 分区聚合
    // 和, 个数, 最大值, 最小值    (平均值到最后一步再算)
    map += "sum" -> (map.getOrElse("sum",0D) + v)
    map += "count" -> (map.getOrElse("count",0D) + 1D)
    map += "max" -> map.getOrElse("max",Double.MinValue).max(v)
    map += "min" -> map.getOrElse("min",Double.MaxValue).min(v)
  }
  // 累加 分区间的合并  其实就是把other中值合并到this的值中
  override def merge(other: AccumulatorV2[Int, Map[String, Double]]): Unit = {
    other match {
      case o: MyAcc =>
      // 把o.map合并到this.map
        map += "sum" -> (map.getOrElse("sum",0D) + o.map.getOrElse("sum", 0D))
        map += "count" -> (map.getOrElse("count",0D) + o.map.getOrElse("count",0D))
        map += "max" -> map.getOrElse("max",Double.MinValue).max(o.map.getOrElse("max",Double.MinValue))
        map += "min" -> map.getOrElse("min",Double.MaxValue).min(o.map.getOrElse("min",Double.MaxValue))
      case _ =>
    }
  }
  // 返回最后累加的值
  override def value: Map[String, Double] = {
    // 这里面计算平均值
    map += "avg" -> (map.getOrElse("sum",0D) / map.getOrElse("count",1D))
    map
  }
}
package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WindowDemo1")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./ck3")
    val stream = ssc.socketTextStream("hadoop102",10000)
    val result = stream.flatMap(_.split(" "))
      .map((_,1))
      // now是新进入的批次的聚合结果, pre离开的批次的聚合结果
      .reduceByKeyAndWindow(_+_,(now,pew) => now - pew,Seconds(9),filterFunc = _._2 > 0)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

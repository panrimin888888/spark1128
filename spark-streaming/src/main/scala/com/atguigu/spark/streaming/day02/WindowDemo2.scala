package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WindowDemo2")
    val ssc = new StreamingContext(conf,Seconds(3))
    ssc.checkpoint("./ck4")

    val stream = ssc.socketTextStream("hadoop102",9999)
      .window(Seconds(9),Seconds(3))
    val wordCount2 = stream.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
    wordCount2.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

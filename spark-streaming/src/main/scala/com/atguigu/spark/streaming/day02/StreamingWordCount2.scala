package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
    val ssc = new StreamingContext(conf, Seconds(2))
    ssc.checkpoint("./ck2")
    val stream = ssc.socketTextStream("hadoop102", 9999)
    val result = stream.flatMap(_.split(","))
      .map((_, 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
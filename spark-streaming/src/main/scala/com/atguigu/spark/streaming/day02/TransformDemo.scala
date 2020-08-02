package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("TransformDemo")
    val ssc = new StreamingContext(conf,Seconds(2))
    val stream = ssc.socketTextStream("hadoop102",9999)
    val wordcount = stream.transform(rdd => {
      rdd.flatMap(_.split(" "))
        .map((_,1))
        .reduceByKey(_+_)
    })
    wordcount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

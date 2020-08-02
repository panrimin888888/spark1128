package com.atguigu.spark.streaming.day01

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf,Seconds(3))

    val stream = ssc.socketTextStream("hadoop102",9999)
    val wordcount = stream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

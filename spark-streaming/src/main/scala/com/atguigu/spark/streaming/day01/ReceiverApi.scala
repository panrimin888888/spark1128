//package com.atguigu.spark.streaming.day01
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object ReceiverApi {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("KafkaUtils")
//    val ssc = new StreamingContext(conf,Seconds(3))
//
//    val sourceStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
//      ssc,
//      "hadoop102:2181,hadoop103:2181,hadoop104:2181/mykafka",
//      "atguigu",
//      Map("spark1128" -> 1))
//    sourceStream
////      .map(_._2)
////        .flatMap(_.split(" "))
////        .map((_,1))
////        .reduceByKey(_+_)
//        .print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}

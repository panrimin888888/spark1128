//package com.atguigu.spark.streaming.day01
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object DirectApi {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectApi")
//    val ssc = new StreamingContext(conf,Seconds(3))
//    val param = Map[String,String](
//      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
//      "group.id" -> "atguigu"
//    )
//    val stream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
//      ssc,
//      param,
//      Set("spark1128")
//    )
//    stream.map(_._2)
//        .flatMap(_.split(" "))
//        .map((_,1))
//        .reduceByKey(_+_)
//        .print
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}

package com.atguigu.spark.streaming.day02

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Direct10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Direct10")
    val ssc = new StreamingContext(conf,Seconds(3))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "atguigu",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    KafkaUtils
      .createDirectStream[String, String](
        ssc,
        PreferConsistent, // 标配
        Subscribe[String, String](Set("spark1128"), kafkaParams))
      .map(_.value())
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print

    ssc.start()
    ssc.awaitTermination()
  }
}

package util

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUtil {
  val params = Map[String,String](
    "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
    "group.id" -> "atguigu"
  )
  def getKafkaStream(ssc: StreamingContext,topic: String,otherTopic: String*) = {
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      params,
      (otherTopic :+ topic).toSet
    ).map(_._2)
  }
}

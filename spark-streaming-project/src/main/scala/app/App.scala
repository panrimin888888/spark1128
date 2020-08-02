package app

import bean.AdsInfo
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.MyKafkaUtil

trait App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("AreaAdsTopApp")
    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.checkpoint("./ck1")

    val adsInfoStream: DStream[AdsInfo] = MyKafkaUtil.getKafkaStream(ssc,"ads_log1128")
      .map(log => {
        val splits = log.split(",")
        AdsInfo(splits(0).toLong,splits(1),splits(2),splits(3),splits(4))
      })

    //封装需求
    doSomething(ssc,adsInfoStream)

    ssc.start()
    ssc.awaitTermination()
  }
  //抽象方法
  def doSomething(ssc: StreamingContext,adsInfoStream: DStream[AdsInfo]):Unit
}

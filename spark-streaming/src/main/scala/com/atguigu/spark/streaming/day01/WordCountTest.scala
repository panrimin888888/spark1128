package com.atguigu.spark.streaming.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCountTest")
    // 1. 创建SparkStreaming的入口对象: StreamingContext
    // 参数2: 表示事件间隔   内部会创建 SparkContext
    val ssc = new StreamingContext(conf,Seconds(3))
    // 2. 创建一个DStream
    val lines = ssc.socketTextStream("hadoop102",9999)
    // 5. 统计单词的个数
    val count = lines.flatMap(_.split("""\s+""")).map((_,1)).reduceByKey(_+_)
    //6. 显示
    count.print
    //7. 开始接受数据并计算
    ssc.start()
    //8. 等待计算结束(要么手动退出,要么出现异常)才退出主程序
    ssc.awaitTermination()

  }
}

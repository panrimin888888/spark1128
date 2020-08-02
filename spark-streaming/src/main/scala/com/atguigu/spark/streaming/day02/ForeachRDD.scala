package com.atguigu.spark.streaming.day02

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDD {
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://hadoop102:3306/rdd"
  val user = "root"
  val pw = "123321"
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ForeachRDD")
    val ssc = new StreamingContext(conf,Seconds(3))

    ssc.checkpoint("./ck5")

    val stream = ssc.socketTextStream("hadoop102",9999)

    val wcStream = stream.flatMap(_.split(" "))
      .map((_,1))
      .updateStateByKey((seq: Seq[Int],opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })

    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()

    import spark.implicits._

    wcStream.foreachRDD(rdd => {
      val df = rdd.toDF("word","count")
      //Class.forName(driver) //加载驱动
      df.write.mode("overwrite").format("jdbc")
        .option("url",url)
        .option("user",user)
        .option("password",pw)
        .option("dbtable","word")
        .save()
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

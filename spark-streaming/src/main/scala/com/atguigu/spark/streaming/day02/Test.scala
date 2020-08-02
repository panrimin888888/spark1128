package com.atguigu.spark.streaming.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test {

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://hadoop102:3306/rdd"
  val user = "root"
  val pw = "123321"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val ssc = new StreamingContext(conf,Seconds(3))

    val stream = ssc.socketTextStream("hadoop102",9999)

    ssc.checkpoint("./ck1")

    //updateStateByKey有状态操作，累计所有值
    val wcStream: DStream[(String, Int)] = stream.flatMap(_.split(" "))
      .map((_,1))
      .updateStateByKey((seq: Seq[Int],opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })

    val spark: SparkSession = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()

    //导入隐式包，把rdd转为df
    import spark.implicits._

    //foreachRDD输出到mysql
    wcStream.foreachRDD(rdd => {
      val df: DataFrame = rdd.toDF("word","count")

      //加载驱动，df会自动加载，mysql驱动包版本得是5.1.27，5.1.2版本得手动加上
      //Class.forName(driver)

      //df写出数据
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

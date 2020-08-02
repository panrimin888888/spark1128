package com.atguigu.spark.core.day05

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

object JdbcWrite {
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://hadoop102:3306/rdd"
  val user = "root"
  val pw = "123321"
  def main(args: Array[String]): Unit = {
    // 把rdd的数据写入到mysql
    val conf: SparkConf = new SparkConf().setAppName("JdbcWrite").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val wordcount = sc.textFile("e:/a.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 3)
    //手动写sql
    val sql = "insert into word_count1128 values(?,?)"
    //分区处理，每个分区连一次就行
    wordcount.foreachPartition(it => {
      // it就是存储的每个分区数据
      // 建立到mysql的连接
      Class.forName(driver)
      // 获取连接
      val conn = DriverManager.getConnection(url, user, pw)
      val ps = conn.prepareStatement(sql)
      var max = 0 // 最大批次
      it.foreach{
        case (word,count) =>
          ps.setString(1, word)
          ps.setInt(2, count)
          ps.addBatch()
          max += 1
          if(max >= 10){
            ps.executeBatch()
            max = 0
          }
      }
      // 最后一次不到提交的上限,  做收尾
      ps.executeBatch()
      conn.close()
    })
    sc.stop()
  }
}

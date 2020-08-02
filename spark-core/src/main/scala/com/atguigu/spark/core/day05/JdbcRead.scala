package com.atguigu.spark.core.day05

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("JdbcRead")
    val sc = new SparkContext(conf)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val user = "root"
    val pw = "123321"
    //获取rdd
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url,user,pw)
        // 千万不要关闭连接
      },
      "select id,name from user where id >= ? and id <= ?",
      1,
      10,
      2,
      row => {
        (row.getInt("id"),row.getString("name"))
      }
    )
    rdd.collect.foreach(println)
    sc.stop()
  }
}

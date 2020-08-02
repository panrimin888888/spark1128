package com.atguigu.spark.sql.day03

import org.apache.spark.sql.SparkSession

object  HiveTest {
  def main(args: Array[String]): Unit = {
    //配置访问权限
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("HiveTest")
      //配置Hive支持
      .enableHiveSupport()
      //配置database的地址
      .config("spark.sql.warehouse.dir","hdfs://hadoop102:9000/user/hive/warehouse")
      .getOrCreate()
    //spark.sql("use gmall")
    //spark.sql("select count(*) from ads_uv_count").show

    //spark.sql("create database spark1128").show //在hive上创建database，到hdfs集群
    spark.sql("use spark1128")  //切换到spark1128数据库
    val df = spark.read.json("e://people.json")

    //写数据到hive中第一种格式：saveAsTable，mode(四种模式)
    //df.write.saveAsTable("user_1")
    //df.write.mode("append").saveAsTable("user_1")
    //spark.sql("select * from user_1").show

    //第二种格式：insertInto，（相当于mode(append)）
    //df.write.insertInto("user_1") // 大致等价于: df.write.mode("append").saveAsTable("user_1")
    //spark.sql("select * from user_1").show

    //第三种格式： 使用 hive的insert 语句
//    spark.sql("insert into table user_1 values('tom',3500)")  //直接追加一个数据
     spark.sql("select * from user_1").show()

    spark.close()
  }

}

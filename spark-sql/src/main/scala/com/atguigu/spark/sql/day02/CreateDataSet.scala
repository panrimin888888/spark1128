package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

object CreateDataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("CreateDS")
      .getOrCreate()
    val list = List(User(12,"jack"),User(15,"tom"),User(19,"jeny"))
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(list)
    val ds = rdd.toDS()
    val rdd1 = ds.rdd
    //创建临时视图
    ds.createOrReplaceTempView("ds_view")
    //查看结构信息
    ds.printSchema()
    //执行sql语句
    spark.sql("select name from ds_view").show()
    //ds.show()
    spark.close()
  }
}
case class User(age: Int,name: String)
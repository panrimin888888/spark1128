package com.atguigu.spark.sql.day02


import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object RDD2DF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]").appName("RDD2DF").getOrCreate()
    val list = List((10,"jack"),(20,"tom"),(15,"jeny"))
    val rdd = spark.sparkContext.parallelize(list)
    var rdd2 = rdd.map{
      case (age,name) => Row(age,name)
    }
    //TODO
      val schema = StructType(Array(StructField("age",IntegerType),StructField("name",StringType)))
      val df = spark.createDataFrame(rdd2,schema)
      df.show()
    spark.close()
  }
}

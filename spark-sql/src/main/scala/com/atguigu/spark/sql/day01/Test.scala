package com.atguigu.spark.sql.day01

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

import scala.collection.immutable.Nil
import scala.tools.cmd.Property

object Test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Test")
      .getOrCreate()
    val df = spark.read.json("e:/people.json")
    val proper = new Properties()
    proper.setProperty("user","root")
    proper.setProperty("password","123321")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop102:3306/rdd","user",proper)
    spark.close()
  }
}

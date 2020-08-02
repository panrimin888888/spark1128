package com.atguigu.spark.sql.day02

import java.text.DecimalFormat


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StringType,LongType, StructField, StructType}

object UDAFDif2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("UDF1")
      .getOrCreate()
    spark.udf.register("my_avg",new MyAVG)
    val df = spark.read.json("e:/people.json")
    df.createOrReplaceTempView("p")
    spark.sql("select my_avg(salary) from p").show
    spark.close()
  }
}
class MyAVG extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("ele",DoubleType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("salary",DoubleType)::StructField("count",LongType)::Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0D
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getDouble(0)+input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0)+buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any =
    new DecimalFormat(".00").format(buffer.getDouble(0) / buffer.getLong(1))
}
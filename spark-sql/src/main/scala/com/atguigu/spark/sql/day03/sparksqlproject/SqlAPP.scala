package com.atguigu.spark.sql.day03.sparksqlproject

import java.text.DecimalFormat

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable
import scala.collection.immutable.Nil

object SqlAPP {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("SqlAPP")
      .enableHiveSupport()
      .config("spark.sql.shuffle.partitions",10)  //默认shuffle或者join是200个分区
      .getOrCreate()
    spark.sql("use sparkpractice")
    spark.udf.register("remark",new CityRemarkUDAF)
    //spark.sql("show tables").show()
    // 1. 先把需要的字段, join, 查出来  t1
    spark.sql("""select
                |  ci.city_name,
                |  ci.area,
                |  pi.product_name,
                |  uva.click_product_id
                |from user_visit_action uva
                |join product_info pi on uva.click_product_id = pi.product_id
                |join city_info ci on uva.city_id = ci.city_id""".stripMargin).createOrReplaceTempView("t1")
    // 2. 按照地区商品分组, 聚合  t2
    spark.sql("""select
                |  area,
                |  product_name,
                |  count(*) count,
                |  remark(city_name) remark
                |from t1
                |group by area,product_name""".stripMargin).createOrReplaceTempView("t2")
    // 3. 开窗, 排序(降序) t3   // rank(1 2 2 4 5)   row_number(1 2 3 4 5...)   dense_rank(1 2 2 3 4)
    spark.sql("""select
                |  area,
                |  product_name,
                |  count,
                |  remark,
                |  rank() over(partition by area order by count desc) rk
                |from t2""".stripMargin).createOrReplaceTempView("t3")
    // 4. top3
    spark.sql("""select
                |  area,
                |  product_name,
                |  count,
                |  remark
                |from t3
                |where rk <= 3""".stripMargin)
      .coalesce(1) //聚合默认是200个分区
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable("result")
      //.show(1000,false)
    spark.close()
  }
}
//城市备注的聚合函数
class CityRemarkUDAF extends UserDefinedAggregateFunction {
  // 输入数据的类型    城市名   StringType
  override def inputSchema: StructType = StructType(StructField("city",StringType)::Nil)
  // 北京->1000 天津->10  Map 缓冲区   MapType(key类型, value类型)
  // 最好再缓冲一个总数
  override def bufferSchema: StructType =
    StructType(StructField("map",MapType(StringType,LongType))::StructField("total",LongType)::Nil)
  // 输出类型  StringType
  override def dataType: DataType = StringType
  //确定性
  override def deterministic: Boolean = true
  // 初始化  对缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String,Long]()
    buffer(1) = 0L
  }

  // 分区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    input match {
      // 传入了一个城市
      case Row(cityName: String) =>
      // 北京
      // 1. 先更新总数
        buffer(1) = buffer.getLong(1) + 1L
      // 2. 再去更新具体城市的数量
        val map = buffer.getMap[String,Long](0)
        buffer(0) = map + (cityName -> (map.getOrElse(cityName,0L) + 1L))
      // 传入了一个null
      case _ =>
    }
  }
  // 分区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 把数据合并, 再放入到buffer1中
    val map1 = buffer1.getMap[String,Long](0)
    val total1 = buffer1.getLong(1)

    val map2 = buffer2.getMap[String,Long](0)
    val total2 = buffer2.getLong(1)
    // 1. 先合并总数
    buffer1(1) = total1+total2
    // 2. 合并map
    buffer1(0) = map1.foldLeft(map2){
      case (map,(city,count)) =>
        map + (city -> (map.getOrElse(city,0L) + count))
    }
  }
  // 返回最终的值
  override def evaluate(buffer: Row): Any = {
    val cityCount = buffer.getMap[String,Long](0)
    val total = buffer.getLong(1)

    // 北京21.2%，天津13.2%，其他65.6%   排序取前2
    val cityCountTop2 = cityCount.toList.sortBy(-_._2).take(2)
    val cityRemarkTop2 = cityCountTop2.map{
      case (city,count) => CityRemark(city,count.toDouble / total)
    }
    // top2 + 其他
    val cityRemark = cityRemarkTop2 :+ CityRemark("其他",cityRemarkTop2.foldLeft(1D)(_-_.rate))
    cityRemark.mkString(",")
  }
}
case class CityRemark(city: String,rate: Double){
  val f = new DecimalFormat(".00%")

  override def toString: String = s"$city:${f.format(rate)}"
}
/*
1.建表
2.导入数据
各区域热门商品 Top3
地区	商品名称		点击次数	城市备注
华北	商品A		100000	北京21.2%，天津13.2%，其他65.6%
华北	商品P		80200	北京63.0%，太原10%，其他27.0%
华北	商品M		40000	北京63.0%，太原10%，其他27.0%
东北	商品J		92000	大连28%，辽宁17.0%，其他 55.0%
--------------------------------------------------------------------------------
1. 先把需要的字段, join, 查出来  t1
select
  ci.city_name,
  ci.area,
  pi.product_name,
  uva.click_product_id
from user_visit_action uva
join product_info pi on uva.click_product_id = pi.product_id
join city_info ci on uva.city_id = ci.city_id
2. 按照地区商品分组, 聚合  t2
select
  area,
  product_name,
  count(*) count
from t1
group by area,product_name
3. 开窗, 排序(降序) t3   // rank(1 2 2 4 5)   row_number(1 2 3 4 5...)   dense_rank(1 2 2 3 4)
select
  area,
  product_name,
  count,
  rank() over(partition by area order by count desc) rk
from t2
4. top3
select
  area,
  product_name,
  count
from t3
where rk <= 3

CREATE TABLE `user_visit_action`(
  `date` string,
  `user_id` bigint,
  `session_id` string,
  `page_id` bigint,
  `action_time` string,
  `search_keyword` string,
  `click_category_id` bigint,
  `click_product_id` bigint,
  `order_category_ids` string,
  `order_product_ids` string,
  `pay_category_ids` string,
  `pay_product_ids` string,
  `city_id` bigint)

CREATE TABLE `product_info`(
  `product_id` bigint,
  `product_name` string,
  `extend_info` string)

CREATE TABLE `city_info`(
  `city_id` bigint,
  `city_name` string,
  `area` string)

 */
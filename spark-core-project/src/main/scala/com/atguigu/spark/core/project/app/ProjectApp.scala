package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.{CategroyCount, UserVisitAction}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

object ProjectApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ProjectApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //1.读数据
    val sourceRDD = sc.textFile("e:/user_visit_action.txt")
    //2.封装到样例类
    val userVisitActionRDD = sourceRDD.map(line =>{
      val splits = line.split("_")
      UserVisitAction(
        splits(0),
        splits(1).toLong,
        splits(2),
        splits(3).toLong,
        splits(4),
        splits(5),
        splits(6).toLong,
        splits(7).toLong,
        splits(8),
        splits(9),
        splits(10),
        splits(11),
        splits(12).toLong)
    })
    //userVisitActionRDD.collect().foreach(println)
    //需求一的返回值，Top10
    //val categoryCountList: List[CategroyCount] = CategoryTopApp.calcCategoryTop10(sc,userVisitActionRDD)
    //需求二的分析
    //CategorySessionTopApp.statCategoryTop10Session(sc,categoryCountList,userVisitActionRDD)
    //需求三
    PageConversionApp.statPageConversionRate(sc,userVisitActionRDD,"1,2,3,4,5,6,7")
    sc.stop()
  }
}

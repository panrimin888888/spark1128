package com.atguigu.spark.coreproject.twice.app

import com.atguigu.spark.coreproject.twice.bean.UserVisitAction2
import org.apache.spark.{SparkConf, SparkContext}

object ProjectAPP2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ProjectAPP2")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("e:/user_visit_action.txt")
    val userVisitActionRDD = rdd.map(line => {
      val splits = line.split("_")
      UserVisitAction2(
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
        splits(12).toLong
      )
    })
    //userVisitActionRDD.collect.foreach(println)
    //需求一
    val categoryCountsList2 = CategoryTopAPP2.calcCategoryTop10(sc,userVisitActionRDD)
    //需求二
    CategorySessionTopApp2.statCategoryTop10Session(sc,categoryCountsList2,userVisitActionRDD)
    sc.stop()
  }
}

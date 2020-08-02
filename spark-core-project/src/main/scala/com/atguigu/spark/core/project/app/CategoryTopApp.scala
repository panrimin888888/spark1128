package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.acc.CategoryAcc
import com.atguigu.spark.core.project.bean.{CategroyCount, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategoryTopApp {
  def calcCategoryTop10(sc: SparkContext,actionRDD: RDD[UserVisitAction]) = {
    //1.创建累加器对象
    val acc = new CategoryAcc
    //2.注册累加器
    sc.register(acc,"CategoryAcc")
    //3.遍历RDD，进行累加
    actionRDD.foreach(action => acc.add(action))
    //4.对数据进行处理top10
    val map = acc.value
//    val result = map
//      .toList
//      .sortBy(x=>x._2)(Ordering.Tuple3(Ordering.Long.reverse,Ordering.Long.reverse,Ordering.Long.reverse))
//      .take(10)
    val categoryList = map.map{
      case (cid,(click,order,pay)) =>
      CategroyCount(cid,click,order,pay)
    }.toList
    val result: List[CategroyCount] = categoryList
      .sortBy(x => (-x.clickCount,-x.orderCount,-x.payCount))
        .take(10)
    //println(result)
    //把需求一的结果返回
    result
  }
}

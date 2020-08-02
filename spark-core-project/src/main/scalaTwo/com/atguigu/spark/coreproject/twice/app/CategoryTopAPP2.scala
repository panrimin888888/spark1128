package com.atguigu.spark.coreproject.twice.app

import com.atguigu.spark.coreproject.twice.acc.CategoryAcc2
import com.atguigu.spark.coreproject.twice.bean.{CategoryCount2, UserVisitAction2}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CategoryTopAPP2 {
  def calcCategoryTop10(sc: SparkContext,
                        actionRDD: RDD[UserVisitAction2]) = {
    //1.创建累加器
    val acc = new CategoryAcc2
    //2.注册累加器
    sc.register(acc,"CategoryAcc2")
    //3.遍历rdd,进行累加
    actionRDD.foreach(action => acc.add(action))
    //4.对数据进行处理top10
    val map = acc.value
    val categoryList = map.map{
      case (cid,(click,order,pay)) => CategoryCount2(cid,click,order,pay)
    }.toList
    val result = categoryList.sortBy(x => (-x.click,-x.order,-x.pay)).take(10)
    //println(result)
    result
  }

}

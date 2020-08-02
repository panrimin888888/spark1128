package com.atguigu.spark.coreproject.twice.app

import com.atguigu.spark.coreproject.twice.bean.{CategoryCount2, SessionInfo2, UserVisitAction2}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CategorySessionTopApp2 {
  def statCategoryTop10Session(sc: SparkContext,
                               categoryCountsList2: List[CategoryCount2],
                               userVisitActionRDD: RDD[UserVisitAction2]) = {
    val cids = categoryCountsList2.map(_.cid.toLong)
    val topCategoryActionRDD = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
    val cidAndSidCount = topCategoryActionRDD
      .map(action => ((action.click_category_id,action.session_id),1))
      .reduceByKey(_+_)
      .map{
        case ((cid,sid),count) => (cid,(sid,count))
      }
    val cidAndSidCountGrouped = cidAndSidCount.groupByKey()
    val result = cidAndSidCountGrouped.map{
      case (cid,it) =>
        var set = mutable.TreeSet[SessionInfo2]()
        it.foreach{
          case (sid,count) =>
            val info = SessionInfo2(sid,count)
            set += info
            if(set.size>10) set = set.take(10)
        }
        (cid,set.toList)
    }
    result.collect.foreach(println)
  }

}

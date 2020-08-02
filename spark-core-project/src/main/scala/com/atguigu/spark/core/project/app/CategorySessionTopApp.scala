package com.atguigu.spark.core.project.app

import com.atguigu.spark.core.project.bean.{CategroyCount, SessionInfo, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object CategorySessionTopApp {
  def statCategoryTop10Session(sc: SparkContext, categoryCountList: List[CategroyCount], userVisitActionRDD: RDD[UserVisitAction]) = {
    val cids = categoryCountList.map(_.cid.toLong)
    val topCategoryActionRDD = userVisitActionRDD.filter(action => cids.contains(action.click_category_id))
    val cidAndSidCount = topCategoryActionRDD
      .map(action => ((action.click_category_id,action.session_id),1))
      .reduceByKey(new CategoryPartitioner(cids),_+_)
      .map{
        case ((cid,sid),count) => (cid,(sid,count))
      }
    //3.排序取top10,mappartitions分区内为一个集体处理
    val result = cidAndSidCount.mapPartitions(it => {
      var treeSet = mutable.TreeSet[SessionInfo]()
      var id = 0L
      it.foreach{
        case (cid,(sid,count)) =>
          id = cid
          treeSet += SessionInfo(sid,count)
          if (treeSet.size > 10) treeSet = treeSet.take(10)
      }
      //treeSet.toIterator.map((id,_))
      Iterator((id,treeSet.toList))
    })
    result.collect.foreach(println)
    Thread.sleep(100000)
  }
}
//自定义分区器,有多少个品类就有多少个分区
class CategoryPartitioner(cids: List[Long]) extends Partitioner{
  private val cidWithIndex = cids.zipWithIndex.toMap
  //放回cids的长度，就是分区数
  override def numPartitions: Int = cids.length

  override def getPartition(key: Any): Int = {
    key match {
        //根据cid返回分区id
      case (cid:Long,_) =>
        //cid % 10 返回0-9分区，不过（比如10，20，30都会进入同一个分区），所以错误
        //cidwithindex转成map,传入k，就可以获取v的值
        cidWithIndex(cid)
    }
  }
}
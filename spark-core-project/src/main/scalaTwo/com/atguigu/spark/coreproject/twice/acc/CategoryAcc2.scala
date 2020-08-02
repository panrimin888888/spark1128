package com.atguigu.spark.coreproject.twice.acc

import com.atguigu.spark.coreproject.twice.bean.UserVisitAction2
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryAcc2 extends AccumulatorV2[UserVisitAction2,mutable.Map[String,(Long,Long,Long)]]{
  //可变map,val就可以
  private val map = mutable.Map[String,(Long,Long,Long)]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction2, mutable.Map[String, (Long, Long, Long)]] = {
    val acc = new CategoryAcc2
    acc.synchronized{
      acc.map ++= this.map
    }
    acc
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(v: UserVisitAction2): Unit = {
    v match {
      case action if action.click_category_id != -1 =>
        //点击cid
        val cid = action.click_category_id.toString
        val (click,order,pay) = map.getOrElse(cid,(0L,0L,0L))
        map.+=(cid->(click+1L,order,pay))
      case action if action.order_category_ids != "null" =>
        val cids = action.order_category_ids.split(",")
        cids.foreach(cid => {
          val (click,order,pay) = map.getOrElse(cid,(0L,0L,0L))
          map.+=(cid->(click,order+1L,pay))
        })
      case action if action.pay_category_ids != "null" =>
        val cids = action.pay_category_ids.split(",")
        cids.foreach(cid => {
          val (click,order,pay) = map.getOrElse(cid,(0L,0L,0L))
          map.+=(cid->(click,order,pay+1L))
        })
      case _ =>
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction2, mutable.Map[String, (Long, Long, Long)]]): Unit = {
    other match {
      case o: CategoryAcc2 =>
        o.map.foreach{
          case (cid,(click,order,pay)) =>
            val (thisclick,thisorder,thispay) = map.getOrElse(cid,(0L,0L,0L))
            this.map += cid->(click+thisclick,order+thisorder,pay+thispay)
        }
      case _ =>
    }
  }

  override def value: mutable.Map[String, (Long, Long, Long)] = map
}

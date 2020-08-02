package com.atguigu.spark.coreproject.twice.bean

case class SessionInfo2(sid: String,count: Int) extends Ordered[SessionInfo2] {
  override def compare(that: SessionInfo2): Int = {
    if(this.count > that.count) -1
    else 1
  }
}

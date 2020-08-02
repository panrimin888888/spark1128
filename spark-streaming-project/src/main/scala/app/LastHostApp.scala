package app
import java.sql.DriverManager

import bean.AdsInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.Serialization

object LastHostApp extends App {

  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://hadoop102:3306/rdd"
  val user = "root"
  val pw = "123321"

  override def doSomething(ssc: StreamingContext, adsInfoStream: DStream[AdsInfo]): Unit = {
    /*
    (ads,hm),1
    (ads,hm),count
    (ads,(hm,count))
     */
    val result = adsInfoStream
      .window(Minutes(60),Seconds(6))
      .map(info => ((info.adsId,info.hmString),1))
      .reduceByKey(_+_)
      .map{
        case ((ads,hm),count) => (ads,(hm,count))
      }
      .groupByKey //把每分钟广告放在一起
      //传输到外部存储
      .foreachRDD((rdd: RDD[(String, Iterable[(String, Int)])]) => {
        //如果主键存在就更新，不存在就插入
        val sql = "insert into lastHost values(?, ?) on duplicate key update hm_count=?"

        //对每个分区的rdd操作
        rdd.foreachPartition(its => {

          //添加驱动
          Class.forName(driver)
          //创建连接
          val conn = DriverManager.getConnection(url,user,pw)

          //对分区里的rdd操作
          its.foreach{
            case (adsId,it) =>

              //把Iterable类型数据转成json
              import org.json4s.DefaultFormats
              val hmCountString = Serialization.write(it.toMap)(DefaultFormats)

              val ps = conn.prepareStatement(sql)

              ps.setInt(1,adsId.toInt)
              ps.setString(2,hmCountString)
              ps.setString(3,hmCountString)
              //启动
            ps.execute()
              //关闭
            ps.close()
          }
          conn.close()
        })
      })

  }
}

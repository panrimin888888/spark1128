package app

import bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AreaAdsTopApp extends App {
  override def doSomething(ssc: StreamingContext, adsInfoStream: DStream[AdsInfo]): Unit = {
    //需求分析
    /*
    1.Dstream[AdsInfo] => ((day,area,ads),1)
    2.(day,area,ads),count
    3.(day,area),(ads,count)
     */
    val result = adsInfoStream.map(info => ((info.dayString,info.area,info.adsId),1))
      .updateStateByKey((seq: Seq[Int],opt: Option[Int]) => {
        Some(seq.sum + opt.getOrElse(0))
      })
      .map{
        case ((day,area,ads),count) => ((day,area),(ads,count))
      }
      .groupByKey
      .mapValues(it => {
        it.toList
          .sortBy(-_._2)
          .take(3)
      })
    result.print()
  }
}

package bean

import java.util.Date
import java.text.SimpleDateFormat
// 1589787737517,华南,深圳,104,4
case class AdsInfo(ts: Long,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String,
                   var dayString: String = null,
                   var hmString: String = null){
  val date = new Date(ts)
  dayString = new SimpleDateFormat("yyyy-MM-dd").format(date)
  hmString = new SimpleDateFormat("HH:mm").format(date)
}

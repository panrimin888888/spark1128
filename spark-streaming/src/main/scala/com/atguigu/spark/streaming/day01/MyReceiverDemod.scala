package com.atguigu.spark.streaming.day01

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}



object MyReceiverDemod {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MyReceiverDemod")
    val ssc = new StreamingContext(conf,Seconds(3))
    val stream = ssc
      .receiverStream(new MyReceiver("hadoop102",9999))
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
    stream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

//Socket来读数据
class MyReceiver(host: String,port: Int) extends Receiver[String](storageLevel = StorageLevel.MEMORY_ONLY) {
  var socket: Socket = _
  var reader: BufferedReader = _
  override def onStart(): Unit = {
    // 防止阻塞onStart, 所以把代码在子线程中执行
    // 从socket读数据
    runInThread {
      try {
        socket = new Socket(host, port)
        reader = new BufferedReader(new InputStreamReader(socket.getInputStream, "utf-8"))
        var line = reader.readLine()
        // 表示读到了数据
        while (line != null) {
          store(line) // 将来spark'会分发其他的executor进行处理
          line = reader.readLine() // 如果没有数据, 这里会阻塞, 等待数据的输入
        }
      } catch {
        case e => println(e.getMessage)
      } finally {
        restart("重启接收器") // 先回调onStop, 再回调 onStart
      }
    }
  }
  /**
   * 用来释放资源
   */
  override def onStop(): Unit = {
    if(socket != null) reader.close()
    if(reader != null) socket.close()
  }

  // 把传入的代码运行在子线程
  def runInThread(op: => Unit) ={
    new Thread(){
      override def run(): Unit = op
    }.start()
  }
}
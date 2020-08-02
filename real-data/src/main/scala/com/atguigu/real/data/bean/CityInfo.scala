package com.atguigu.real.data.bean

import scala.collection.immutable

case class CityInfo(city_id: Long,
                    city_name: String,
                    area: String
                   )

object Test{
  def main(args: Array[String]): Unit = {
    val name: String = "jack"
    val age: Int = 18
    val sex: String = "男"
    val salary: Double = 22000.00
    val salary2: Double = 25000.00
    val company: String = "shuwei1"
    val company2: String = "guigu"
    val stringList = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")
    val strings: immutable.Seq[String] = stringList.flatMap(x => x.split(" "))
    val wordTogetor = strings.groupBy(word=>word)
    val wordCount = wordTogetor.map(x=>(x._1,x._2.size))
    wordCount.toList.sortBy(-_._2).foreach(println)
  }
}

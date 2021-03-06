package com.bigdata.nciae.location

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/12.
 */
object AdUserLocation {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UserLocation").setMaster("local")
    val sc = new SparkContext(conf)

    /**
     * 字段：手机号码,事件时间,基站ID,事件类型 eg.
     * 18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
     * now  begin to read file to rdd
     */

    val original_data = sc.textFile("data//bg_log")

    /**
     * eg .(18688888888,16030401EAFB68F1E3CDF819735E1C66),-20160327082400)
     */
    val original_data_formate = original_data.map(line => {
      val field = line.split(",")
      val mobile = field(0)
      val eventTime = field(1)
      val lac = field(2)
      val eventType = field(3)
      val time = if (eventType == "1") -eventTime.toLong else eventTime.toLong
      //返回一个元组
      ((mobile, lac), time)
    })

    /**
     * ((18688888888,CC0710CC94ECC657A8561DE549D940E0),1300)
     */
    val rdd = original_data_formate.reduceByKey(_ + _).map(t => (t._1._2, (t._1._1, t._2)))
    val rdd2 = sc.textFile("data//loc_info.txt").map(_.split(",")).map(t => (t(0), (t(1), t(2))))
    val rdd3 = rdd.join(rdd2).map(t => (t._1, t._2._1._1, t._2._1._2, t._2._2._2, t._2._2._1))
    val rdd4 = rdd3.groupBy(_._2).mapValues(t => {
      t.toList.sortBy(_._3).reverse.take(2)
    })
    rdd4.saveAsTextFile("c://out2")
    sc.stop()
  }


}

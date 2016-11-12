package com.bigdata.nciae.location

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/12.
 *
 * 根据基站 信息 判断手机用户家庭地址和工作地址
 */
object MobileLocaltion {

  def main(args: Array[String]) {


    val conf = new SparkConf().setAppName("UserLocation").setMaster("local")
    val sc = new SparkContext(conf)

    /**
     * 字段：手机号码,事件时间,基站ID,事件类型 eg.
     * 18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
     * now  begin to read file to rdd
     */

    val original_data = sc.textFile("c://bg_log")
    /**
     * eg .(18688888888_16030401EAFB68F1E3CDF819735E1C66,20160327082400)
     */
    val original_data_formate = original_data.map(line => {

      val field = line.split(",")
      val mobile = field(0)
      val eventTime = field(1)
      val lac = field(2)
      val eventType = field(3)
      val time = if (eventType == "1") -eventTime.toLong else eventTime.toLong
      //返回一个元组
      (mobile + "_" + lac, time)
    })

    //print(original_data_formate.collect().toBuffer)

    val group_fomate = original_data_formate.groupBy(_._1)

    // print(group_fomate.collect().toBuffer)
    /**
     * eg.ArrayBuffer((18611132889_9F36407EAD0629FC166F14DDE7970F68,54000), ...))
     */
    val rdd = group_fomate.mapValues(_.foldLeft(0l)(_ + _._2))

    //print(rdd.collect.toBuffer)

    val rdd1 = rdd.map(t => {
      val mobileAndlac = t._1
      val time = t._2
      val field = mobileAndlac.split("_")
      (field(0), field(1), time)
    })


    // println(rdd1.collect().toBuffer)
    val rdd2 = rdd1.groupBy(_._1)
    val rdd3 = rdd2.mapValues(t => {
      //内部排序
      t.toList.sortBy(_._3).reverse.take(2)
    })
    //   println(rdd3.collect().toBuffer)

    rdd3.saveAsTextFile("c://out")
    sc.stop()
  }

}

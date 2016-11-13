package com.bigdata.nciae.iplocation

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Rainbow on 2016/11/13.
 * 查找 ip归属地
 */
object IpLocaltion {

  //ip String to Long

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //二分查找

  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("IpLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //读取比较规则 并广播到各个Worker
    val ipRulesRDD = sc.textFile("c://ip.txt").map(t => {
      val field = t.split("\\|")
      val ip_start = field(2)
      val ip_end = field(3)
      val province = field(6)
      (ip_start, ip_end, province)
    })

    val ipRulesArray = ipRulesRDD.collect()
   // println(ipRulesArray.toBuffer)
    //广播规则
    val broadcastRules = sc.broadcast(ipRulesArray)

    //读取目标数据

    val dataFile = sc.textFile("c://data.format").map(t => {

      val field = t.split("\\|")
      val ip = field(1)
      (ip)
    })

    val infoRDD = dataFile.map(ip => {

      val ipLong = ip2Long(ip)
      val index = binarySearch(broadcastRules.value, ipLong)
      val info = broadcastRules.value(index)
      (info._3,ip)
    })
     val result= infoRDD.groupBy(_._1)

     println(result.collect().toBuffer)

  //  infoRDD.saveAsTextFile("c:\\out3")


    sc.stop()


  }

}

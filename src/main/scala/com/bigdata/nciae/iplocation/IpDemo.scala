package com.bigdata.nciae.iplocation

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by Rainbow on 2016/11/13.
 *
 * 读取本地Ip规则库 二分查询
 */


object IpDemo {

  //read ip rule from local
  def readData(filePath: String) = {

    val textFile = Source.fromFile("c://ip.txt")
     textFile.getLines().toArray[String]
  }

  //ip String to Long

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  //二分排序

  def binarySearch(lines: Array[String], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle).split("\\|")(2).toLong) && (ip <= lines(middle).split("\\|")(3).toLong))
        return middle
      if (ip < lines(middle).split("\\|")(2).toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }


  def main(args: Array[String]) {
    val ip = "123.197.64.48"
    val ipNum = ip2Long(ip)
    println(ipNum)
    val lines = readData("c://ip.txt")
   // println(lines.toBuffer)
    val index = binarySearch(lines, ipNum)
    print(lines(index))
  }

}

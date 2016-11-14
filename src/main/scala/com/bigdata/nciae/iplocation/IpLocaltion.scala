package com.bigdata.nciae.iplocation

import java.sql.{Date, PreparedStatement, Connection, DriverManager}

import org.apache.spark.{SparkContext, SparkConf}
/**
 * Created by Rainbow on 2016/11/13.
 * 查找 ip归属地
 */
object IpLocaltion {

  //data->MySql

  def dataToMySql(iterator: Iterator[(String, Int)]) = {
    var conn: Connection = null;
    var prestament: PreparedStatement = null;
    val sql="insert into location_info(location,count,create_time) values(?,?,?)"

    try{
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=UTF-8","root","root")

      iterator.foreach(line=>{
        prestament=conn.prepareStatement(sql)
        prestament.setString(1,line._1)
        prestament.setInt(2,line._2)
        prestament.setDate(3,new Date(System.currentTimeMillis()))
        prestament.executeUpdate()
      })
    }catch {
      case  e:Exception=>println(e.getLocalizedMessage)
    }finally {

      if(prestament!=null) prestament.close()
      if(conn!=null)  conn.close()

    }
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
      (info._3, 1)
    })
    val result = infoRDD.reduceByKey(_ + _).sortBy(_._2, false)
     //数据保存在mysql
    result.foreachPartition(dataToMySql)
    //println(result.collect().toBuffer)

    //  infoRDD.saveAsTextFile("c:\\out3")
    sc.stop()
  }

}

package com.bigdata.nciae.topUrl

import org.apache.spark.Partitioner

import scala.collection.mutable

/**
 * Created by Rainbow on 2016/11/13.
 *
 * 实现自定义的分区
 */
class HostPartition(val inst: Array[String]) extends Partitioner {

  //分区数量等于学院的数组长度
  override def numPartitions: Int = inst.length

  //Array[String]->Map[String,Int] that easy for get partition location number
  val hostMap = new mutable.HashMap[String, Int]()
  var index = 0
  for (instname <- inst) {
    //hostMap.put(instname,index)
    hostMap += (instname -> index)
    index += 1
  }

  //following key to get Partition Number
    override def getPartition(key: Any): Int = {
       hostMap.getOrElse(key.toString,0)
    }
  //following key to get Partition Number
//  override def getPartition(key: Any): Int = key match {
//      case null => 0
//      case key: String if (hostMap.contains(key)) => hostMap(key)
//      case _ => 0
//    }
}

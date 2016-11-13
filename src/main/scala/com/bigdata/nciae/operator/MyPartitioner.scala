package com.bigdata.nciae.operator

import org.apache.spark.Partitioner

/**
 * Created by Rainbow on 2016/11/13.
 *
 * 自定义 Partitioner
 */
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions


  //  override def getPartition(key: Any): Int = {
  //
  //    key match {
  //      case null => 0
  //      case key: Int => key % numPartitions
  //      case _ => key.hashCode % numPartitions
  //    }
  //  }

  override def getPartition(key: Any): Int = {

    key match {
      case null => 0
      case "java" => 1
      case "php" => 2
      case _ => 0
    }
  }

  override def equals(other: Any): Boolean = other match {

    case h: MyPartitioner => true
    case _ => false
  }
}

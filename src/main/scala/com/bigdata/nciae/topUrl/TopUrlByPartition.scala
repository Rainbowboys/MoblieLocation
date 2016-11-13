package com.bigdata.nciae.topUrl

import java.net.URL

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/13.
 */
object TopUrlByPartition {

  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("TopUrl").setMaster("local[2]")

    val sc = new SparkContext(config)

    val textFile = sc.textFile("c://itcast.log")

    val rdd = textFile.map(line => {
      val filed = line.split("\t")
      (filed(1), 1)
    })
    val rdd1 = rdd.reduceByKey((_ + _), 1)

    val rdd2 = rdd1.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    })

    val inst = rdd2.map((_._1)).distinct().collect()
    // print(rdd3.collect().toBuffer)
    val hostPatitioner = new HostPartition(inst)
    rdd2.partitionBy(hostPatitioner).mapPartitions(t=>{
      t.toList.sortBy(_._2._2).reverse.take(3).iterator
    }).map(t=>(t._1,t._2._1,t._2._2)).saveAsTextFile("c://out2")
    sc.stop()
  }

}

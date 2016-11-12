package com.bigdata.nciae.topUrl

import java.net.URL

import org.apache.spark.{SparkContext, SparkConf}

/**
 *
 * 取出学科点击前三的
 * Created by Rainbow on 2016/11/12.
 */
object TopUrl {


  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("TopUrl").setMaster("local[2]")

    val sc = new SparkContext(config)

    val textFile = sc.textFile("c://itcast.log")

    val rdd = textFile.map(line => {
      val filed = line.split("\t")
      (filed(1), 1)
    })
    val rdd1 = rdd.reduceByKey((_ + _),1)

    val rdd2 = rdd1.map(t => {
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2)
    })

    val rdd3 = rdd2.groupBy(_._1).mapValues(t => {
      t.toList.sortBy(_._3).reverse.take(3)
    })
    rdd3.saveAsTextFile("c://out")
   // print(rdd3.collect().toBuffer)
    sc.stop()


  }

}

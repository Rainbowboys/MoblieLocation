package com.bigdata.nciae.topUrl

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/12.
 */
object TopUrlOnTotal {

  def main(args: Array[String]) {
    val config = new SparkConf().setAppName("TopUrl").setMaster("local[2]")

    val sc = new SparkContext(config)

    val textFile = sc.textFile("c://itcast.log")

    val rdd = textFile.map(_.split("\t")).map(f => (f(1), 1)).reduceByKey(_+_).sortBy(_._2,false).take(3)
    println(rdd.toBuffer)

  }


}

package com.bigdata.nciae.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Rainbow on 2016/11/12.
 * Spark WordCount Demo
 */
object WordCount {

  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("WordCount").setMaster("local")

    val sc = new SparkContext(config)
    val textFile = sc.textFile(args(0))
    //val textFile = sc.textFile("D://test.txt")
    textFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey((_ + _)).sortBy(_._2, true).saveAsTextFile(args(1))
    sc.stop()

  }


}

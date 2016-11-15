package com.bigdata.nciae.Stream

import org.apache.log4j.Level
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/15.
 */


object StreamWordCount {

  def main(args: Array[String]) {
    //设置日志级别
    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val conf = new SparkConf().setAppName("StreamWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val ssc = new StreamingContext(sc, Seconds(5))

    val ds = ssc.socketTextStream("192.168.145.147", 8888)

    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()


    ssc.start()
    ssc.awaitTermination()

  }


}

package com.bigdata.nciae.Stream

import java.net.InetSocketAddress

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Rainbow on 2016/11/15.
 *
 * Spark Streaming poll message from flume
 */
object FlumePollWordCount {

  def main(args: Array[String]) {

    LoggerLevels.setStreamingLogLevels(Level.WARN)
    val conf = new SparkConf().setAppName("FlumePollWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val address = Seq(new InetSocketAddress("192.168.145.203", 8888))
    val flumeStream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK)
    val result = flumeStream.flatMap(x => new String(x.event.getBody.array()).split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

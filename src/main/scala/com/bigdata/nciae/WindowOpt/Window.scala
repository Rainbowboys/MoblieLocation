package com.bigdata.nciae.WindowOpt

import com.bigdata.nciae.Stream.LoggerLevels
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Rainbow on 2016/11/16.
 */
object WindowOpt {

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels(Level.WARN)
    val sparkConf = new SparkConf().setAppName("WindowOpt").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val socketText = ssc.socketTextStream("192.168.145.203", 9999)
    val words = socketText.flatMap(_.split(" ")).map((_, 1))
    val windowedWordCounts = words.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(30), Seconds(10))
    windowedWordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

package com.bigdata.nciae.Stream

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Rainbow on 2016/11/15.
 *
 * Flume 向 Spark Streaming 推送消息
 */
object FlumePushWordCount {

  def main(args: Array[String]) {

    val host=args(0)
    val port=args(1).toInt
    LoggerLevels.setStreamingLogLevels(Level.WARN)
    val conf = new SparkConf().setAppName("FlumePushWordCount")//.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val flumeStream = FlumeUtils.createStream(ssc, host, port)

    //flume中的数据通过X.event.getBody()才能拿到真正的内容
    val result = flumeStream.flatMap(x => new String(x.event.getBody.array()).split(" ")).map((_, 1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()

  }

}

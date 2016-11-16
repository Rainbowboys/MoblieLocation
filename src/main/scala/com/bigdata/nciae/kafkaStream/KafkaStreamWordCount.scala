package com.bigdata.nciae.kafkaStream

import com.bigdata.nciae.Stream.LoggerLevels
import org.apache.log4j.Level
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Rainbow on 2016/11/15.
 */
object KafkaStreamWordCount {

  val updatefunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
     iter.flatMap{case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(m => (x, m))}
    //iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
   // iter.flatMap { case (word, current_word, history) => Some(current_word.sum + history.getOrElse(0)).map(m => (word, m)) }
  }

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val conf = new SparkConf().setAppName("KafkaStreamWordCount").setMaster("local[2]")
    val topMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val ssc = new StreamingContext(conf, Seconds(5))
    //使用updateStateByKey 必须设置ckeckpoint
    ssc.checkpoint("c://ck-bigdata")

    //get DsStream
    val ds = KafkaUtils.createStream(ssc, zkQuorum, groupId, topMap)
    //deal ds
    val result = ds.flatMap(_._2.split(" ")).map((_, 1)).updateStateByKey(updatefunc, new HashPartitioner(ssc.sparkContext.defaultMinPartitions), true)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

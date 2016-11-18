package com.bigdata.nciae.Stream

import com.bigdata.nciae.JedisConnectionPool
import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * Created by Rainbow on 2016/11/17.
 * Spark Stream 检测外挂
 */
object ScannPlugin {


  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val conf = new SparkConf().setAppName("ScannPlugin").setMaster("local[*]")

    val dateFormat: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val topMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("D://ck-gamelog9")
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )


    val ds = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topMap, StorageLevel.MEMORY_AND_DISK)
    val streamOrignal: DStream[Array[String]] = ds.map(_._2).map(_.split("\t"))
    val lines = streamOrignal.filter(f => {
      if (f.size == 13) {
        val et = f(3)
        val it = f(8)
        et == "11" && it == "强效太阳水"

      } else false
    })

    val groupwindow: DStream[(String, Iterable[Long])] = lines.map(f => (f(7), dateFormat.parse(f(12)).getTime)).groupByKeyAndWindow(Milliseconds(30000), Seconds(15000))

    val timesMore: DStream[(String, Iterable[Long])] = groupwindow.filter(_._2.size > 3)
    val avg = timesMore.mapValues(t => {
      val list = t.toList.sorted
      val size = list.size
      val first = list(0)
      val last = list(size - 1)
      (last - first) / size
    })

    val badUser: DStream[(String, Long)] = avg.filter(_._2 < 1000)
    badUser.foreachRDD(rdd => {
      rdd.foreachPartition(t => {
        val connection: Jedis = JedisConnectionPool.getConnection()
        t.foreach(x => {
          val key = x._1
          val time = x._2
          val current_time = System.currentTimeMillis()
          connection.set(key + "_" + current_time, time.toString)
        })
        connection.close()
      })

    })
    lines.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

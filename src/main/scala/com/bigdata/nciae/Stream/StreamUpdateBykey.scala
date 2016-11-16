package com.bigdata.nciae.Stream

import org.apache.log4j.Level
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/15.
 * 累计计算结果
 */
object StreamUpdateBykey {
  //updateFunc: (Iterator[(K, Seq[V], Option[S])]) => Iterator[(K, S)]
  //Iterator[(String, Seq[Int], Option[Int])]
  /**
   * Iterator[(String, Seq[Int], Option[Int])] 一个参数代表 单词的值 第二参数seq[Int]表示已经分组的(1,1,1,1,1)
   * 第三参数代表上一次的结果 或者初始值 eg (hello,(1,1,1),Option[5])
   * @return
   */
  val updatefunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
   // iter.flatMap{case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(m => (x, m))}
    //iter.map(t=>(t._1,t._2.sum+t._3.getOrElse(0)))
    iter.flatMap{case(word,current_word,history)=>Some(current_word.sum+history.getOrElse(0)).map(m=>(word,m))}
  }

  def main(args: Array[String]) {

    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val conf = new SparkConf().setAppName("StreamWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //使用updateStateByKey 必须设置ckeckpoint
    sc.setCheckpointDir("c:\\ck20161115")
    val ssc = new StreamingContext(sc, Seconds(5))
    val ds = ssc.socketTextStream("192.168.145.203", 8888)

    val result = ds.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updatefunc, new HashPartitioner(sc.defaultParallelism), true)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

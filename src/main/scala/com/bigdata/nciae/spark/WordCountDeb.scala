package com.bigdata.nciae.spark

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/14.
 */
object WordCountDeb {


    def main(args: Array[String]) {
      //非常重要，是通向Spark集群的入口
      val conf = new SparkConf().setAppName("WC")
       .setJars(Array("E:\\BaiduYunDownload\\Spark-WordCount\\target\\Spark-WordCount-1.0-SNAPSHOT.jar"))
        .setMaster("spark://master:7077")
      val sc = new SparkContext(conf)

      //textFile会产生两个RDD：HadoopRDD  -> MapPartitinsRDD
      sc.textFile(args(0)).cache()
        // 产生一个RDD ：MapPartitinsRDD
        .flatMap(_.split(" "))
        //产生一个RDD MapPartitionsRDD
        .map((_, 1))
        //产生一个RDD ShuffledRDD
        .reduceByKey(_+_)
        //产生一个RDD: mapPartitions
        .saveAsTextFile(args(1))
      sc.stop()


}
}

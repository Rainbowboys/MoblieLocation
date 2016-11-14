package com.bigdata.nciae.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Rainbow on 2016/11/12.
 * Spark WordCount Demo   WordCount程序 流程 分析 与rdd 依赖
 */
object WordCount {

  def main(args: Array[String]) {

    val config = new SparkConf().setAppName("WordCount")
      .setJars(Array("E:\\BaiduYunDownload\\Spark-WordCount\\target\\Spark-WordCount-1.0-SNAPSHOT.jar")).setMaster("spark://master:7077")


    val sc = new SparkContext(config)

    //[0]HadoopRDD[K,V] <-new HadoopRDD(...).setName(path)
    //we only need V so to map -> [1]HadoopRDD.map(pair => pair._2.toString)
    //get two RDD
   // val textFile = sc.textFile(args(0))
    val textFile=sc.parallelize(List(" the local radio station. He often told"))
   // val textFile = sc.textFile("c://wc")
    //[2]MapPartitionsRDD =new MapPartitionsRDD
    val rdd = textFile.flatMap(_.split(" "))
    // [3]MapPartitionsRDD=new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
    val rdd1 = rdd.map((_, 1))
    //  [4]new ShuffledRDD[K, V, C]
    val rdd2 = rdd1.reduceByKey((_ + _))
    //  this.keyBy[K](f) .sortByKey(ascending, numPartitions)  执行这条语句 产生三个RDD  MapPartitionsRDD[5] -> ShuffledRDD[6]->new MapPartitionsRDD[7]
    val rdd3 = rdd2.sortBy(_._2, true)
     println(rdd3.collect().toBuffer)
   //this.mapPartitions->MapPartitionsRDD
    //rdd3.saveAsTextFile("D:/out")
    sc.stop()

  }


}

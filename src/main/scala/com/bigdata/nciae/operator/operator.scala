package com.bigdata.nciae.operator

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/13.
 */
object operator {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("UserLocation").setMaster("local")
    val sc = new SparkContext(conf)


    val rdd1 = sc.parallelize(List("java", "php","Hadoop"), 2)
    val rdd2 = sc.parallelize(List(2, 2,3), 2)
    val rdd3=rdd1.zip(rdd2)
    //val rdd2 = rdd1.keyBy(_.length)
    val partitioner = new MyPartitioner(3)
    val rdd4 = rdd3.groupByKey(partitioner)

    val func = (index: Int, iter: Iterator[(String,Iterable[Int])]) => {
      iter.toList.map(x => "[partId:" + index + ",val:" + x + "]").iterator
    }

    println(rdd4.mapPartitionsWithIndex(func).collect().toBuffer)

    sc.stop()

  }


}

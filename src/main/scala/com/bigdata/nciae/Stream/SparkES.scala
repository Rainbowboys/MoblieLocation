package com.bigdata.nciae.Stream

import com.bigdata.nciae.JedisConnectionPool
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import redis.clients.jedis.Jedis

/**
 * Created by Rainbow on 2016/11/18.
 * Spark 查询 ES 形成RDD
 */
object SparkES {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("SparkES").setMaster("local[*]")
      .set("es.nodes", "zookeeper01,zookeeper02,zookeeper03")
      .set("es.port", "9200")
      .set("es.index.auto.create", "true")

    val sc = new SparkContext(sparkConf)
    val query =
      """{
        |"query" : {
        |        "filtered" : {
        |            "query" : {
        |                "match_all" : {}
        |            },
        |            "filter" : {
        |                "term" : {
        |                    "price" : 35.99
        |                  }
        |              }
        |        }
        |    }
        |}

      """.stripMargin
    val rdd = sc.esRDD("store", query)
    rdd.foreach(t=>{
      val connection: Jedis = JedisConnectionPool.getConnection()
      val resultMap=t._2
      val title= resultMap.get("title")
      connection.set("title",title.toString)
      connection.close()
    })

    //println(result.collect().toBuffer)
    sc.stop()
  }


}

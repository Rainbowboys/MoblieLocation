package com.bigdata.nciae.SqlDemo

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, IntegerType, StructType}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/14.
 *
 * 使用 schema 创建 DataFrame
 */
object SQLDemoBySchema {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    System.setProperty("user.name", "master")

    val personRdd = sc.textFile("hdfs://master:9000/person.txt").map(_.split(","))
    val schema = StructType({
      List(
        StructField("id", IntegerType, true)
        , StructField("name", StringType, true)
        , StructField("age", IntegerType, true)
      )
    })

    val rowRdd = personRdd.map(x => Row(x(0).toInt, x(1).trim, x(2).toInt))

     val df=  sqlContext.createDataFrame(rowRdd, schema)

     df.show()

     df.write.json("c://out")
     sc.stop()
  }

}

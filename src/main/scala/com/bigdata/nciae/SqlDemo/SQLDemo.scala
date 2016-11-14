package com.bigdata.nciae.SqlDemo

import org.apache.spark.sql.SQLContext

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Rainbow on 2016/11/14.
 * Spark SQL Demo
 */
object SQLDemo {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //设置hadoop 权限
    System.setProperty("user.name","master")
    val personRdd=sc.textFile("hdfs://master:9000/person.txt").map(line=>{
      val field=line.split(",")
       Person(field(0).toLong,field(1),field(2).toInt)
    })
    //toDF 的隐式转换
    import sqlContext.implicits._
    val personDf=personRdd.toDF()
    personDf.registerTempTable("tb_person")
    personDf.show()
   // sqlContext.sql("select * from tb_person").show
    sc.stop()
  }
  case class Person( Id:Long, Name:String,Age:Int)

}

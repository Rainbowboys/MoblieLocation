package com.bigdata.nciae.JDBC

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/13.
 *
 * select data from database into rdd
 */


object JdbcRDDDemo {
  //get jdbc conn

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("jdbc").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val connection = () => {
      val url: String = "jdbc:mysql://localhost:3306/bigdata?useUnicode=true&characterEncoding=UTF-8"
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection(url, "root", "root")
    }

    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "select * from ta where id >=? AND id<=?",
       1, 5, 2,
      rs => {
        val id = rs.getInt(1)
        val code = rs.getString(2)
        (id, code)
      }

    )
    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }

}

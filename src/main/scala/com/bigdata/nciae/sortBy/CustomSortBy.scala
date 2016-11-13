package com.bigdata.nciae.sortBy

import com.bigdata.nciae.topUrl.Girl
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Rainbow on 2016/11/13.
 */

object myPreDef {

  //function for T->Ordered[T]
  implicit val CartOrdering = new Ordering[Girl]  {
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValues==x.faceValues){
        x.age-y.age
      }else{
        x.faceValues-x.faceValues
      }
    }
  }
}


object CustomSortBy {

  def main(args: Array[String]) {

    import com.bigdata.nciae.sortBy.myPreDef._
    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("Yuihatano", 80, 23), ("Jizhemingbu", 80, 24), ("Baby", 90, 25)))
    //引入 隐式转换门面 sortBy 自行在上下文查找比较规则
    val result = rdd.sortBy(t => Girl(t._2, t._3), false)

    println(result.collect().toBuffer)

    sc.stop()
  }

}

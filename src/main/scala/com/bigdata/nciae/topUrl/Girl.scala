package com.bigdata.nciae.topUrl

/**
 * Created by Rainbow on 2016/11/13.
 *
 * this oo . we need def a rule to sort
 *
 * first to compare faceValues then if faceValues equal compare age
 */


  /*first Method  */
//case class Girl(val faceValues:Int,val age:Int) extends  Ordered[Girl] with Serializable{
//  override def compare(that: Girl): Int ={
//     if(this.faceValues==that.faceValues){
//       this.age-this.age
//     }else{
//       this.faceValues-that.faceValues
//     }
//  }
//}

case class Girl(faceValues:Int,age:Int) extends  Serializable

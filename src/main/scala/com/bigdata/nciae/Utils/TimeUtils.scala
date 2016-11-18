package com.bigdata.nciae.Utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Created by Rainbow on 2016/11/17.
 *
 */
object TimeUtils {

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val calendar = Calendar.getInstance()

  //time:Stirng ->time:Long Millis
  def apply(time: String): Long = {
    calendar.setTime(sdf.parse(time))
    calendar.getTimeInMillis
  }

  def getCertainDayTime(amount: Int):Long = {
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -amount)
    time
  }
}

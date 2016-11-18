package com.bigdata.nciae.Utils

import org.apache.commons.lang3.time.FastDateFormat


/**
 * Created by Rainbow on 2016/11/17.
 */
object FilterUtils {


  val dataFormate = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

  def filterByDay(fields: Array[String], startTime: Long, endTime: Long): Boolean = {
    val time = fields(1)
    val longTime = dataFormate.parse(time).getTime
    longTime >= startTime && longTime < endTime
  }

  def filterByType(fields: Array[String], eventType: String) = {
    val _type = fields(0)
    _type == eventType
  }

  def filterByTypes(fields: Array[String], eventType: String*): Boolean = {
    val _type = fields(0)
    for (elem <- eventType) {
      if (_type == elem) return true
    }
    false
  }

  def filterByTimeAndTypes(fields: Array[String], startTime: Long, endTime: Long, eventType: String): Boolean = {
    val time = fields(1)
    val _type = fields(0)
    val longTime = dataFormate.parse(time).getTime
    eventType == _type && longTime >= startTime && longTime < endTime

  }
}

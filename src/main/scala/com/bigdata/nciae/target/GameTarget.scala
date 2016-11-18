package com.bigdata.nciae.target

import com.bigdata.nciae.Stream.LoggerLevels
import com.bigdata.nciae.Utils.{EventType, TimeUtils, FilterUtils}
import org.apache.log4j.Level
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Rainbow on 2016/11/17.
 * 游戏管理的各项指标计算
 */
object GameTarget {

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels(Level.WARN)

    val time = "2016-02-02 00:00:00"
    val todayTime = TimeUtils(time)
    val endTime = TimeUtils.getCertainDayTime(1)
    val yesterDay=TimeUtils.getCertainDayTime(-1)

    val sparkConf = new SparkConf().setAppName("GameTarget").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val gamelogs = sc.textFile("D:\\GameLog.txt").map(_.split("\\|"))
    val todayUser = gamelogs.filter(fields => FilterUtils.filterByDay(fields, todayTime, endTime)).cache()

    //日新增用户数，Daily New Users 缩写 DNU
    val dnu = todayUser.filter(fileds => FilterUtils.filterByType(fileds, EventType.REGISTER)).count()

    //日活跃用户数 DAU （Daily Active Users）
    val dau=todayUser.filter(fields => FilterUtils.filterByTypes(fields,EventType.REGISTER,EventType.LOGIN)).map(_(3)).distinct().count()


    //次日留存率
    val yesterdayRegister=gamelogs.filter(fields=>FilterUtils.filterByTimeAndTypes(fields,yesterDay,todayTime,EventType.REGISTER))
    //tuple(name,1) for join by totay
    val yesterdayRegUser=yesterdayRegister.map(x=>(x(3),1))
    val todayLoginUser=todayUser.filter(fields=>FilterUtils.filterByType(fields,EventType.LOGIN)).map(x=>(x(3),1)).distinct()
    val d1r:Double= yesterdayRegUser.join(todayLoginUser).count()
    val d1rr = d1r / yesterdayRegUser.count()
    println(dnu)
    println(dau)
    println(d1rr)
    sc.stop()
  }

}

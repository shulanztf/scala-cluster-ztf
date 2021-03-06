package com.msb.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @description: 数据输出处理类.
 * @author: zhaotf
 * @create: 2020-07-05 20:14
 */
class WriteDataSink[T](classType: Class[_ <: T]) extends RichSinkFunction[T] {
  var conn: Connection = _
  var pst: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    conn = DriverManager.getConnection("jdbc:mysql://192.168.1.133:3306/hlhtpoint?useUnicode=true&characterEncoding=utf-8&useSSL=false", "root", "123456")
    if (classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)) {
      pst = conn.prepareStatement("insert into t_speeding_info (car,monitor_id,road_id,real_speed,limit_speed,action_time) values (?,?,?,?,?,?)")
    }
    if (classType.getName.equals(classOf[AvgSpeedInfo].getName)) {
      pst = conn.prepareStatement("insert into t_average_speed (start_time,end_time,monitor_id,avg_speed,car_count) values (?,?,?,?,?)")
    }
    if (classType.getName.equals(classOf[RepetitionCarWarning].getName)) {
      pst = conn.prepareStatement("insert into t_violation_list (car,violation,create_time) values (?,?,?)")
    }
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }

  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
    if (classType.getName.equals(classOf[OutOfLimitSpeedInfo].getName)) {
      val info: OutOfLimitSpeedInfo = value.asInstanceOf[OutOfLimitSpeedInfo]
      pst.setString(1, info.car)
      pst.setString(2, info.monitorId)
      pst.setString(3, info.roadId)
      pst.setDouble(4, info.realSpeed)
      pst.setInt(5, info.limitSpeed)
      pst.setLong(6, info.actionTime)
      pst.executeUpdate()
    }
    if (classType.getName.equals(classOf[AvgSpeedInfo].getName)) {
      val info: AvgSpeedInfo = value.asInstanceOf[AvgSpeedInfo]
      pst.setLong(1, info.start)
      pst.setLong(2, info.end)
      pst.setString(3, info.monitorId)
      pst.setDouble(4, info.avgSpeed)
      pst.setInt(5, info.carCount)
      pst.executeUpdate()
    }
    if (classType.getName.equals(classOf[RepetitionCarWarning].getName)) {
      val info: RepetitionCarWarning = value.asInstanceOf[RepetitionCarWarning]
      var sdf = new SimpleDateFormat("yyyy-MM-dd")
      val day: Date = sdf.parse(sdf.format(new Date(info.actionTime))) //当前的0时0分0秒
      //在一天中，每个车牌只写入一次
      var selectPst = conn.prepareStatement("select count(1) from t_violation_list where car=? and create_time between ? and ?")
      selectPst.setString(1, info.car)
      selectPst.setLong(2, day.getTime)
      selectPst.setLong(3, day.getTime + (24 * 60 * 60 * 1000))
      val selectSet: ResultSet = selectPst.executeQuery()
      if (selectSet.next() && selectSet.getInt(1) == 0) { //当天没有该车牌号,插入
        pst.setString(1, info.car)
        pst.setString(2, info.msg)
        pst.setLong(3, info.actionTime)
        pst.executeUpdate()
      }
      selectSet.close()
      selectPst.close()

    }
  }
}

package com.msb.util

import java.time.LocalDateTime

import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.types.Row
import java.time.ZoneId

/**
 * @description: flink-mysql.
 * @author: zhaotf
 * @create: 2020-07-11 17:38
 */
object OperatorMysql {
  val url = "jdbc:mysql://192.168.1.133:3306/hlhtpoint?useUnicode=true&characterEncoding=utf-8"
  val username = "root"
  val password = "123456"


  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //    val driver = "com.mysql.jdbc.Driver"
    val sql_write = "insert into t_violation_list (car,violation,create_time) values (?,?,?)"
    val curTime = LocalDateTime.now()
    val outputData:DataSet[Row] = env.fromElements(("ttt", "0", curTime.atZone(ZoneId.systemDefault).toInstant.toEpochMilli))
      .map(x => {
        val row = new Row(3)
        row.setField(0, x._1)
        row.setField(1, x._2)
        row.setField(2, x._3)
        row
      })
    writeMysql( outputData,  sql_write)
    env.execute("insert data to mysql")
  }

  // 写mysql
  def writeMysql( outputData: DataSet[Row], sql: String) = {
    outputData.output(JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl(url)
      .setUsername(username)
      .setPassword(password)
      .setQuery(sql)
      .finish())
    print("data write successfully")
  }

  /**
   * https://www.bbsmax.com/A/l1dyRLBAze/ flink写入mysql的两种方式
   * @param query
   */
  def  writeMysqlFull(query: String ): Unit = {
     JDBCOutputFormat.buildJDBCOutputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl(url)
      .setUsername(username)
      .setPassword(password)
      .setQuery(query)
      .finish();
  }
}

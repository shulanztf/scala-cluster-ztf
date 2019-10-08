package com.sutdy.flink.streaming.scala.examples.socket

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}

import scala.language.postfixOps


/**
  * https://blog.csdn.net/aA518189/article/details/83992129,flink实战开发----flinkSQL入门大全
  */
object sql_test {

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //获取table
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //读取数据源
    val source1 = env.readTextFile("/data/flink/flink_data/person.txt")
    val source2: DataStream[Person1] = source1.map(x => {
      val split = x.split(" ")
      (Person1(split(0), split(1)))
    })
    //    将DataStream转化成Table
    val table1: Table = tableEnv.fromDataStream(source2)
    //    注册表，表名为：person
    tableEnv.registerTable("persion", table1)
    //    获取表中所有信息
    val rs: Table = tableEnv.sqlQuery("select * from persion ")
    val stream: DataStream[String] = rs.select("name") //过滤获取name这一列的数据
      .toAppendStream[String] // 将表转化成DataStream

    stream.print()
    env.execute("flinkSQL")
  }


}


/**
  * 定义样例类封装数据,模式匹配
  *
  * @param name
  * @param score
  */
case class Person1(name: String, score: String)

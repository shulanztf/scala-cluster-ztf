package com.api.study.flink.sql

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.slf4j.LoggerFactory

import scala.language.postfixOps

/**
  * @ClassName: TestSql2
  * @Author: zhaotf
  * @Description:
  * @Date: 2019/12/4 0004 
  */
class TestSql2 {

}

object TestSql2 {
  val logger = LoggerFactory.getLogger(TestSql2.getClass)

  def main(args: Array[String]): Unit = {
    //获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //获取table
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //读取数据源
    val source1 = env.readTextFile("/data/flink/flink-data/person.txt")


    val source2: DataStream[Person1] = source1.map(x => {
      val split = x.split(" ")
      (Person1(split(0), split(1)))
    })

    val table1: Table = tableEnv.fromDataStream(source2) // 将DataStream转化成Table
    tableEnv.registerTable("persion", table1) // 注册表，表名为：person

    val rs: Table = tableEnv.sqlQuery("select * from persion ") // 获取表中所有信息
    val stream: DataStream[String] = rs.select("name") //过滤获取name这一列的数据
      .toAppendStream[String] // 将表转化成DataStream

    stream.writeAsText("/data/flink/file",FileSystem.WriteMode.OVERWRITE)// 写到目标文件，OVERWRITE覆盖，生产慎用
        .setParallelism(1)// 设置分区，避免多个文件
    stream.print()
    env.execute("TestSql2")
  }
}

/**
  * 定义样例类封装数据,模式匹配
  *
  * @param name
  * @param score
  */
case class Person1(name: String, score: String)
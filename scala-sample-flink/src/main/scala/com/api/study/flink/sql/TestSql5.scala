package com.api.study.flink.sql

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author 赵腾飞
  * @date 2019/12/8/008 19:33
  */
class TestSql5 {

}

/**
  * flink-sql 双流join,stream版
  * https://blog.csdn.net/yanghua_kobe/article/details/73143367 Flink-Table-SQL系列之source
  */
object TestSql5 {
  val logger: Logger = LoggerFactory.getLogger(TestSql5.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    //    数据源
    val source1: DataStream[(String, String)] = env.readTextFile("/data/flink/flink-data/person.txt").map(x => {
      val split = x.split("\t")
      (split(0), split(1))
    })
    val source2: DataStream[(String, Int)] = env.readTextFile("/data/flink/flink-data/user.txt").map(x => {
      val split = x.split("\t")
      (split(0), split(1).toInt)
    })

    // 注册表、字段
    tableEnv.registerDataStream("table1", source1, 'name, 'score)
    tableEnv.registerDataStream("table2", source2, 'name, 'level)
    val table3: Table = tableEnv.sqlQuery(
      """
        |select t1.name as name, t1.score as score,
        |       t2.level as level
        |  from table1 as t1
        |  left join table2   as t2
        |         on t1.name = t2.name
        | where 1=1
      """.stripMargin)

    val result3: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table3) // 注意，必须Row,否则报java.lang.NullPointerException: Null result cannot be stored in a Case Class.
    result3.writeAsText("/data/flink/file", WriteMode.OVERWRITE) //覆盖现有文件，生产慎用
      .setParallelism(1) //设置分区为1，避免多个文件
    //        result3.print() // 不要与execute并用，会有执行两次错误

    env.execute("TestSql5")
  }

}

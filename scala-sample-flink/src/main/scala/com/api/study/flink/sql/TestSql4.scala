package com.api.study.flink.sql

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

/**
  * @author 赵腾飞
  * @date 2019/12/7/007 12:41
  */
class TestSql4 {

}

/**
  * flink-sql 双流join,batch版
  * https://blog.csdn.net/yanghua_kobe/article/details/73143367 Flink-Table-SQL系列之source
  */
object TestSql4 {
  val logger = LoggerFactory.getLogger(TestSql4.getClass)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    //    数据源
    val source1: DataSet[(String, String)] = env.readTextFile("/data/flink/flink-data/person.txt").map(x => {
      val split = x.split("\t")
      (split(0), split(1))
    })
    val source2: DataSet[(String, Int)] = env.readTextFile("/data/flink/flink-data/user.txt").map(x => {
      val split = x.split("\t")
      (split(0), split(1).toInt)
    })
    tableEnv.registerDataSet("table1", source1, 'name, 'score)
    tableEnv.registerDataSet("table2", source2, 'name, 'level)
    val table3: Table = tableEnv.sqlQuery(
      """
        |select t1.name as name, t1.score as score,
        |       t2.level as level
        |  from table1 as t1
        |  left join table2   as t2
        |         on t1.name = t2.name
        | where 1=1
      """.stripMargin)

    val result3 = tableEnv.toDataSet[Row](table3) // 注意，必须Row,否则报java.lang.NullPointerException: Null result cannot be stored in a Case Class.
    result3.writeAsText("/data/flink/file", WriteMode.OVERWRITE) //覆盖现有文件，生产慎用
      .setParallelism(1) //设置分区为1，避免多个文件
    //        result3.print() // 不要与execute并用，会有执行两次错误

    env.execute("TestSql4")
  }
}

/**
  * 定义样例类封装数据,模式匹配
  *
  * @param name
  * @param score
  */
//case class Person3(name: String, score: String)
//
//case class User(name:String,level:Int)

case class ResultTable(name: String, score: String, level: Int)

package com.api.study.flink.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.sinks.CsvTableSink
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.CsvTableSource

/**
  * @author 赵腾飞
  * @date 2019/12/7/007 12:41
  */
class TestSql4 {

}

/**
  * 不可行
  * https://blog.csdn.net/yanghua_kobe/article/details/73143367 Flink-Table-SQL系列之source
  */
object TestSql4 {
  val logger = LoggerFactory.getLogger(TestSql4.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    //    数据源
    val source1:CsvTableSource = CsvTableSource
      .builder
      .path("/data/flink/flink-data/person.csv")
      .field("name", Types.STRING)
      .field("score", Types.STRING)
//      .fieldDelimiter("#")
//      .lineDelimiter("$")
      .ignoreFirstLine
      .ignoreParseErrors
      .commentPrefix("%")
      .build()
    val source2:CsvTableSource=CsvTableSource
    .builder
      .path("/data/flink/flink-data/person.csv")
      .field("name", Types.STRING)
      .field("level", Types.INT)
//      .field("score", Types.DOUBLE)
//      .field("comments", Types.STRING)
//      .fieldDelimiter("#")
//      .lineDelimiter("$")
      .ignoreFirstLine
      .ignoreParseErrors
      .commentPrefix("%").build()
//    val source1= new CsvTableSource("/data/flink/flink-data/person.txt",)
//    val source2:DataStream[(User)] = env.readTextFile("/data/flink/flink-data/user.txt").map(x=>{
//      val split = x.split("\t")
//      (User(split(0),split(1).toInt))
//    })
//    val table1:Table = tableEnv.fromDataStream(source1)
//    val table2:Table = tableEnv.fromDataStream(source2)
    tableEnv.registerTableSource("table1", source1)//注册表
    tableEnv.registerTableSource("table2", source2)



    val sqlResult3:Table = tableEnv.sqlQuery(
      """
        |select t1.name,t1.score
        |       ,t2.level
        |  from table1 as t1
        |  left join table2   as t2
        |         on t1.name = t2.name
        | where 1=1
      """.stripMargin)

    val result3 = sqlResult3.toRetractStream[ResultTable]
    result3.print()
//    tableEnv.registerTable("sqlResult3",sqlResult3)
////    sqlResult3.printSchema()
//    val result3 = sqlResult3.toRetractStream[ResultTable] //
//    result3.print()
    result3.writeAsText("/data/flink/file",WriteMode.OVERWRITE)//覆盖现有文件，生产慎用
        .setParallelism(1)//设置分区为1，避免多个文件
    env.execute("TestSql4")
  }
}

/**
  * 定义样例类封装数据,模式匹配
  *
  * @param name
  * @param score
  */
case class Person3(name: String, score: String)

case class User(name:String,level:Int)

case class ResultTable(name:String,score:String,level:Int)

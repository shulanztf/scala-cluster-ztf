package com.api.study.flink.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.catalog.{ExternalCatalog, InMemoryExternalCatalog}
import org.apache.flink.table.sinks.{CsvTableSink, TableSink}

/**
  * @ClassName: TestSql3
  * @Author: zhaotf
  * @Description:
  * @Date: 2019/12/4 0004 
  */
class TestSql3 {

}

/**
  * https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/table/common.html#register-a-table
  * flink sql
  */
object TestSql3 {
  val logger = LoggerFactory.getLogger(TestSql3.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

//    数据源
    val source1:DataStream[(Person3)] = env.readTextFile("/data/flink/flink-data/person.txt").map(x=>{
      val split = x.split(" ")
      (Person3(split(0), split(1)))
    })
    val source2:DataStream[(User)] = env.readTextFile("/data/flink/flink-data/user.txt").map(x=>{
      val split = x.split("\t")
      (User(split(0),split(1).toInt))
    })

    val table1:Table = tableEnv.fromDataStream(source1)
    val  table2:Table = tableEnv.fromDataStream(source2)
//    val catalog: ExternalCatalog = new InMemoryExternalCatalog("TestSql3Catalog")

    // register a Table
    tableEnv.registerTable("table1", table1)//注册表
    tableEnv.registerTable("table2", table2)     // or
//    tableEnv.registerExternalCatalog("extCat", catalog)


////    val tapiResult:Table = tableEnv.scan("table1").select("name","score")// 为支持SQL语言，不建议使用此api
//    val tapiResult:Table = tableEnv.sqlQuery("select * from table1  where score like 'z%' ")
////    val sqlResult:Table  = tableEnv.sqlQuery("SELECT name,sum(level) as level FROM table2  where level > 10 group by name ")//推荐使用
//    val sqlResult:Table  = tableEnv.sqlQuery("SELECT * FROM table2  where level > 10  ")//推荐使用
    val sqlResult3:Table = tableEnv.sqlQuery(
      """
        |select t1.name,t1.score
        |       ,t2.level
        |  from table1 as t1
        |  left join table2   as t2
        |         on t1.name = t2.name
        | where 1=1
        |   and t1.score like 'z%'
      """.stripMargin)

//    val result1 = tapiResult.toAppendStream[Person3] //仅查询时，可用追加模式toAppendStream
//    result1.print()
//    val result2 = sqlResult.toAppendStream[(String,Int)]// 使用聚合函数时，用缩进模式toRetractStream
//    result2.print()
    val result3 = sqlResult3.toRetractStream[ResultTable]
    result3.print()

//    数据输出
// get a TableEnvironment

    // create a TableSink
    val csvSink: CsvTableSink = new CsvTableSink("/data/flink/file")

    // define the field names and types
    val fieldNames: Array[String] = Array("name", "score", "level")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.STRING, Types.INT)

    // register the TableSink as table "CsvSinkTable"
    tableEnv.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, csvSink)
    sqlResult3.insertInto("CsvSinkTable")




    env.execute("TestSql3")
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
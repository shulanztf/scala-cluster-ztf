package com.api.study.flink.sql

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, TableSchema, Types}
import org.apache.flink.table.sources.{DefinedProctimeAttribute, StreamTableSource}
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

/**
  * @ClassName: TestSql1
  * @Author: zhaotf
  * @Description:
  * @Date: 2019/12/4 0004 
  */
class TestSql1 {

}

/**
  * https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/table/common.html#explaining-a-table
  * flink-sql解释表执行计划
  */
object TestSql1 {
  val logger = LoggerFactory.getLogger(TestSql1.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._
    val table1: Table = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table2: Table = env.fromElements((1, "hello")).toTable(tEnv, 'count, 'word)
    val table = table1
      .where('word.like("F%"))
      .unionAll(table2)

    val explanation: String = tEnv.explain(table)
    println(explanation)
  }

}



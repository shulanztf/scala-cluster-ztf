package com.sutdy.df

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://www.cnblogs.com/ilinuxer/p/6851652.html Spark-Sql之DataFrame实战详解
  * spark-sql相关
  */
class DataFrameTest {

}

object DataFrameTest {

  def main(args: Array[String]): Unit = {
//    日志显示级别
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.ERROR)

//    初始化
    val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("/data/spark/source/spark-sql-people.json")

//    查看df中的数据
    df.show()
//  查看Schema
    df.printSchema()
    //      查看某个字段
    df.select("name").show()
//  查看多个字段，plus为加上某值
    df.select(df.col("name"),df.col("age").plus(1),df.col("addr")).show()
//    过滤某个字段的值
    df.filter(df.col("age").gt(25)).show()
//    count group 某个字段的值
    df.groupBy("age").count().show()
    //foreach 处理各字段返回值
    df.select(df.col("id"),df.col("name"),df.col("age")).foreach(x=>{
      //通过下标获取数据
      println("col1:",x.get(0),", col2:name:",x.get(1),",addr",x.get(2))
    })
    //foreachPartition 处理各字段返回值，生产中常用的方式
    df.select(df.col("id"),df.col("name"),df.col("age")).foreachPartition(list => {
      list.foreach(x => {
        //通过字段名获取数据
        println("id:",x.getAs("id"),",age:name:",x.getAs("name"),",age:",x.getAs("age"))
      })
    })

  }




































}

package com.sutdy.spark

import org.apache.avro.generic.GenericData.StringType
import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.types.{DataTypes, LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession, types}

/**
  * spark-sql
  *
  * @see https://www.cnblogs.com/hadoop-dev/p/6742677.html
  */
class LocalSparkSql {

}

object LocalSparkSql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalSparkSql").setMaster("local")
    val sc = new SparkContext(conf)
    //    val sql = new SQLContext(sc)
    val sql = SparkSession.builder().getOrCreate()
    val people = sql.read.json("D:\\data\\spark\\people.txt") //people是一个DataFrame类型的对象

    //数据读进来了，那我们查看一下其schema吧
    people.schema


    //以数组的形式分会schema
    people.dtypes
    //返回schema中的字段
    people.columns



    //    以tree的形式打印输出schema
    people.printSchema()


  }
}

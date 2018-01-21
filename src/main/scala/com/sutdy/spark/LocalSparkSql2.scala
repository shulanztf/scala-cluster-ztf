package com.sutdy.spark

import org.apache.derby.iapi.sql.conn.SQLSessionContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-sql,用编程的方式指定模式
  *
  * @see http://www.infoq.com/cn/articles/apache-spark-sql
  */
class LocalSparkSql2 {

}

object LocalSparkSql2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalSql").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 用已有的Spark Context对象创建SQLContext对象
    val sqlContext = SparkSession.builder().getOrCreate()
    // 创建RDD对象
    val rddCustomers = sc.textFile("D:\\data\\spark\\customers.txt")
    // 用字符串编码模式
    val schemaString = "customer_id name city state zip_code"

    // 导入Spark SQL数据类型和Row
    import org.apache.spark.sql._
    import org.apache.spark.sql.types._

    // 用模式字符串生成模式对象
    //    val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val schema = StructType(schemaString.split(" ").map((line: String) => {
      StructField(line, StringType, true)
    }))
    // 将RDD（rddCustomers）记录转化成Row。
    //    val rowRDD = rddCustomers.map(_.split(",")).map(p => Row(p(0).trim,p(1),p(2),p(3),p(4)))
    val rowRDD = rddCustomers.map((line: String) => {
      line.split(",")
    }).map(p => {
      Row(p(0).trim, p(1), p(2), p(3), p(4))
    })
    // 将模式应用于RDD对象
    val dfCustomers = sqlContext.createDataFrame(rowRDD, schema)
    // 将DataFrame注册为表
    //    dfCustomers.registerTempTable("")
    dfCustomers.createOrReplaceTempView("customers")
    // 用sqlContext对象提供的sql方法执行SQL语句。
    val cusNames = sqlContext.sql("SELECT name FROM customers")
    // SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
    // 可以按照顺序访问结果行的各个列。
    cusNames.map(t => {
      "Name:" + t(0)
    }).collect().foreach(println)
    // 用sqlContext对象提供的sql方法执行SQL语句。
    val customersByCity = sqlContext.sql("SELECT name,zip_code FROM customers ORDER BY zip_code")
    // SQL查询的返回结果为DataFrame对象，支持所有通用的RDD操作。
    // 可以按照顺序访问结果行的各个列。
    customersByCity.map(t => {
      t(0) + "," + t(1)
    }).collect().foreach(println)


  }

}


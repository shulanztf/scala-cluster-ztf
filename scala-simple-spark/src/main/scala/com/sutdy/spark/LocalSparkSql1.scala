package com.sutdy.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import org.slf4j.event.Level

/**
  * spark-sql
  *
  * @see http://www.infoq.com/cn/articles/apache-spark-sql
  */
class LocalSparkSql1 {

}

object LocalSparkSql1 {

  // 创建一个表示客户的自定义类
  case class Customer(customer_id: Int, name: String, city: String, state: String, zip_code: String)

  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("LocalSql").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 首先用已有的Spark Context对象创建SQLContext对象
    val sqlContext = SparkSession.builder().getOrCreate()
    //    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // 导入语句，可以隐式地将RDD转化成DataFram
    import sqlContext.implicits._
    // 用数据集文本文件创建一个Customer对象的DataFrame
    val dfCustomers = sc.textFile("D:\\data\\spark\\customers.txt").map(_.split(",")).map(pe => Customer(pe(0).trim.toInt, pe(1), pe(2), pe(3), pe(4))).toDS()
    // 将DataFrame注册为一个表
    //    dfCustomers.registerTempTable("")
    dfCustomers.createOrReplaceTempView("customers")
    // 显示DataFrame的内容
    dfCustomers.show()
    // 打印DF模式
    dfCustomers.printSchema()
    // 选择客户名称列
    dfCustomers.select("name").show()
    // 选择客户名称和城市列
    dfCustomers.select("name", "city").show()
    // 根据id选择客户
    dfCustomers.filter(dfCustomers("customer_id").equalTo(500)).show()
    // 根据邮政编码统计客户数量
    dfCustomers.groupBy("zip_code").count().show()

  }


}

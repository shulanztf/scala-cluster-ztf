package com.msb.util

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Durations, StreamingContext}

object SparkSessionBase {

  def createSparkSession() = {
    val session = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .config("hive.metastore.uris", "thrift://hserver134:9083")
      .enableHiveSupport()
      .getOrCreate()
     session
//    val prop = new Properties()
//    val inputStream = SparkSessionBase.getClass.getClassLoader.getResourceAsStream("spark-conf.properties")
//    prop.load(inputStream)
//    val conf = new SparkConf()
//
//    conf.set("spark.default.parallelism","10")
//   /* val settings = List(
//        ("spark.app.name", prop.getProperty("spark.app.name"))
//        ("spark.master", "local")
//        ("spark.executor.cores", prop.getProperty("spark.executor.cores"))
//        ("spark.executor.instances",prop.getProperty("spark.executor.instances"))
//        ("hive.metastore.uris",prop.getProperty("hive.metastore.uris"))
//    )*/
//    conf.setAppName(prop.getProperty("spark.app.name"))
//    conf.setMaster("local")
//    conf.set("hive.metastore.uris",prop.getProperty("hive.metastore.uris"))
//
//    val enableHive = prop.getProperty("enable.hive.support","false").toBoolean
//    if (enableHive)
//      SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
//    else
//      SparkSession.builder().config(conf).getOrCreate()
  }

  def createStreamingContext() ={
    val prop = new Properties()
    val inputStream = SparkSessionBase.getClass.getClassLoader.getResourceAsStream("spark-conf.properties")
    prop.load(inputStream)
    val sparkConf = new SparkConf()
    sparkConf.setAppName(prop.getProperty("spark.streaming.app.name"))
    sparkConf.setMaster("local[3]")
    sparkConf.set("hive.metastore.uris",prop.getProperty("hive.metastore.uris"))
    new StreamingContext(sparkConf,Durations.seconds(1))
  }
}

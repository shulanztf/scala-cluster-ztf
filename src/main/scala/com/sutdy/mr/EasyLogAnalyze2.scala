package com.sutdy.mr

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: https://blog.csdn.net/liu16659/article/details/81254911 Spark案例实战之三
  * Auther: Administrator
  * Date: 2018/11/14 0014 17:42
  */
object EasyLogAnalyze2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EasyLogAnalyze2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val text: RDD[String] = sc.textFile("/data/spark/source/EasyLogAnalyze")
    text.foreach(println)

    val res1: RDD[(String, Int)] = text.filter(f => {
      StringUtils.isNotBlank(f)
    }).map(line => {
      process(line)
    })
    val res2: RDD[(String, Int)] = res1.reduceByKey(_ + _)
    res2.sortBy(_._2).collect().foreach(println)
  }

  def process(line: String): (String, Int) = {
    var symbol: String = null
    if (StringUtils.isNotBlank(line)) {
      symbol = line.substring(0, line.indexOf(" "))
    }
    (symbol, 1)
  }

}

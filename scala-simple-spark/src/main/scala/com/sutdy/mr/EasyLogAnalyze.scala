package com.sutdy.mr

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/liu16659/article/details/81254911  Spark案例实战之三
  */
object EasyLogAnalyze {
//  var blankLines = 0

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EasyLogAnalyze").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val text: RDD[String] = sc.textFile("/data/spark/source/EasyLogAnalyze")
    text.foreach(println)

    //    1.line是参数
    //    2.{}中的内容是函数处理步骤
    val res1: RDD[(String, Int)] = text.filter(f => {
      StringUtils.isNotBlank(f)
    }).map(line => {
      var symbol: String = null
      if (StringUtils.isNotBlank(line)) {
        //取首字符串
        symbol = line.substring(0, line.indexOf(" "))
      } else {
//        blankLines += 1
      }
      (symbol, 1)
    })

    val res2: RDD[(String, Int)] = res1.reduceByKey(_ + _)
    res2.sortBy(_._2).foreach(println)

  }

}

package com.sutdy.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/51440251 Hadoop经典案例Spark实现（七）——日志分析：分析非结构化文件
  */
object mapreduceD7 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hhcf-sparkPai4").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fourth = sc.textFile("/data/spark/source/seven", 3) // 读取文件，并，设置并行数量

    //filter 过滤长度小于0， 过滤不包含GET与POST的URL
    val filterRDD = fourth.filter(f => {
      f.trim.length > 0 && (f.indexOf("GET") > 0 || f.indexOf("POST") > 0)
    })
    //转换成键值对操作
    filterRDD.map(line => {
      if (line.indexOf("GET") > 0) {
        (line.substring(line.indexOf("GET"), line.indexOf("HTTP/1.0")), 1)
      } else {
        (line.substring(line.indexOf("POST"), line.indexOf("HTTP/1.0")), 1)
      }
    }).reduceByKey(_ + _) //最后通过reduceByKey求sum
      //        .foreach(f => {
      //      println("rdd:",f._1,f._2)
      //    })
      .collect() //触发action事件执行
      .foreach(f => {
      println("aa:", f)
    })
  }

}

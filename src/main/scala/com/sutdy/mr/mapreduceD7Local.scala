package com.sutdy.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/51440251 Hadoop经典案例Spark实现（七）——日志分析：分析非结构化文件
  * 自实现
  */
object mapreduceD7Local {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hhcf-sparkPai4").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fourth = sc.textFile("/data/spark/source/seven", 3) // 读取文件，并，设置并行数量

    fourth.filter(f => {
      //过滤
      f.trim.length > 0 && (f.contains("GET") || f.contains("POST"))
    }).map(line => {
      // KV转换，并，转成二元组，准备计算
      if (line.contains("GET")) {
        (line.substring(line.indexOf("GET"), line.indexOf("HTTP/1.0")).trim, 1)
      } else {
        (line.substring(line.indexOf("POST"), line.indexOf("HTTP/1.0")).trim, 1)
      }
    }).reduceByKey((x, y) => {
      // 计算请求次数
      x + y
    }).sortBy(line => {
      // 排序,降序
      line._2
    }, false).collect().foreach(f => {
      println("bbc:", f)
    })


  }

}

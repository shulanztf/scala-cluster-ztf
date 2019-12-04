package com.sutdy.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/50483207 Hadoop经典案例Spark实现（五）——求最大最小值问题
  */
object mapreduceD5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hhcf-sparkPai4").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fourth = sc.textFile("/data/spark/source/five", 3) // 读取文件，并，设置并行数量
    fourth.filter(t => {
      t.trim.length > 0
    }).map(line => {
      ("key", line.trim.toInt)
    }).groupByKey().map(line => {
      var max = Integer.MIN_VALUE
      var min = Integer.MAX_VALUE

      for (num <- line._2) {
        if (num > max) {
          max = num
        }
        if (num < min) {
          min = num
        }
      }
      (max, min)
    }).collect().foreach(x => {
      println("max:", x._1, ",min:", x._2)
    }
    )
  }

}

package com.sutdy.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/50464343 Hadoop经典案例Spark实现（一）——通过采集的气象数据分析每年的最高温度
  */
class mapreduceD1 {

}


object mapreduceD2{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hhcf-sparkPai1").setMaster("local[3]")
    val sc = new SparkContext(conf)
    var l = sc.parallelize(1 to 10).map(x => x * x).count()
    println("zzz", l)

  }
}

object mapreduceD1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hhcf-sparkPai1").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val fourth = sc.textFile("/data/spark/source/one.txt", 3) // 读取文件，并，设置并行数量

    val yearAndTemp = fourth.filter(line => {
      val quality = line.substring(50, 51);
      var airTemperature = 0
      if (line.charAt(45) == "+") {
        airTemperature = line.substring(46, 50).toInt
      } else {
        airTemperature = line.substring(45, 50).toInt
      }
      airTemperature != 9999 && quality.matches("[01459]")
    }).map(line => {
      val year = line.substring(15, 19)
      var airTemperature = 0
      if (line.charAt(45) == "+") {
        airTemperature = line.substring(46, 50).toInt
      } else {
        airTemperature = line.substring(45, 50).toInt
      }
      (year, airTemperature)
    })

    val res = yearAndTemp.reduceByKey((x, y) => {
      if (x > y) x else y
    })

    res.collect().foreach(f => {
      println("year:", f._1, ",max:", f._2)
    })
  }
}






































package com.sutdy.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/50474763 Hadoop经典案例Spark实现（二）——数据去重问题
  */
//object mapreduceD2 {

//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("hhcf-sparkPai1").setMaster("local[3]")
//    val sc = new SparkContext(conf)
//    val fourth = sc.textFile("/data/spark/source/two", 3) // 读取文件，并，设置并行数量
//    fourth.filter(f => {
//      f.trim.length > 0
//    }).map(line => {
//      (line.trim, "") //转换成二元组集合
//    }).groupByKey().sortByKey().keys.collect().foreach(f => {
//      //通过collect()算子，将各分区/RDD数据，集中到一个集合内
//      //    }).groupByKey().sortByKey().keys.foreach(f => {
//      println("--", f)
//    })
//  }

//}

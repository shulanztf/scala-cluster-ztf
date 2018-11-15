package com.sutdy.mr

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: https://blog.csdn.net/liu16659/article/details/81254921 Spark案例实战之四
  * https://blog.csdn.net/qq_21383435/article/details/77371142  spark学习-SparkSQL--12-SparkSession与SparkContext
  * Auther: Administrator
  * Date: 2018/11/15 0015 10:21
  */
object WeiboStatisticsLocal {

  /**
    * top5，按粉丝数，由高到低:栏目 [(用户,粉丝数),(用户,粉丝数)]
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WeiboStatistics").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val text = sc.textFile("/data/spark/source/WeiboStatistics")

    val res1: RDD[String] = text.filter(StringUtils.isNotBlank(_))
    val res2: RDD[Array[String]] = res1.map(_.split(" "))

    val res3: RDD[(String, String, Int)] = res2.map(x => {
      //      x(0)栏目名，x(1)用户/主播,x.length - 2粉丝数
      (x(0), x(1), x.length - 2)
    })
    res3.foreach(println)

    res3.getNumPartitions

    res3.sortBy(_._3, false) //降序
      .map(x => {
      //      转成（栏目名，（主播，粉丝数））格式
      (x._1, (x._2, x._3))
    }).groupByKey().collect().foreach(println)

  }

}

package com.sutdy.mr


import org.apache.commons.lang3.StringUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Description:https://blog.csdn.net/liu16659/article/details/81254921 Spark案例实战之四
  * https://blog.csdn.net/qq_21383435/article/details/77371142  spark学习-SparkSQL--12-SparkSession与SparkContext
  * Auther: Administrator
  * Date: 2018/11/14 0014 17:56
  */
object WeiboStatistics {

  def main(args: Array[String]): Unit = {
    //    val ss: SparkSession = SparkSession.builder().appName("WeiboStatistics").master("local[2}").getOrCreate()
    //    val rdd1:RDD[String] = ss.sparkContext.textFile("")
    //    val csvRDD = ss.read.option("header","true").csv("")
    //    csvRDD.filter(_!=null).map((_,1))

//    val conf = new SparkConf().setAppName("WeiboStatistics").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val text = sc.textFile("/data/spark/source/WeiboStatistics")
//
//    val res1: RDD[String] = text.filter(StringUtils.isNotBlank(_))
//    val res2: RDD[Array[String]] = res1.map(_.split(" "))
//    var fansNum = 0
//
//    val res3: RDD[(String, String, Int)] = res2.map(x => {
//      (x(0), x(1), x.length - 2)
//    })
//    res3.foreach(println)
//
//    val res4: RDD[((String, String), Int)] = res3.map(x => {
//      ((x._1, x._2), x._3)
//    })
//    val res5 = res4.sortBy(_._2, false).groupBy(_._1._1)
//    res5.collect().foreach(println("aaa", _))

    val conf = new SparkConf().setAppName("ShareVariables").setMaster("local")
    val sc = new SparkContext(conf)

    val text = sc.textFile("/data/spark/source/WeiboStatistics")
    var res1: RDD[Array[String]] = text.map(line => line.split(" "))//split line with space
    var fansNum = 0

    val res2 = res1.map(x => (x(0),x(1),x.length-2) )//get x(0) x(1) x.length-2 form a map
    res2.collect.foreach(println) //print

    val res3: RDD[((String, String), Int)] = res2.map(x => ((x._1,x._2),x._3))
    val res4 = res3.sortBy(x=>x._2,false).groupBy(x=> x._1._1)
    res4.foreach(println)
  }

}






































package com.sutdy.simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.commons.lang.StringUtils

/**
  * spark-wordCount
  */
class SxtFilterCountOperator {

}

object SxtFilterCountOperator {

  def main(args: Array[String]): Unit = {
    //创建Spark运行时的配置对象，在配置对象里可以设置APP Name,集群URL及运行时的各种资源需求
    val sparkConf = new SparkConf().setAppName("MapOperator").setMaster("local")
    //创建sparkContext上下文环境，通过 传入配置对象，实例化一个SparkContext
    val sc = new SparkContext(sparkConf)

    /**
      * 读取文件,数据格式
      * 2017-01-01  HHCF  1323  青岛
      * 2017-01-01  HMLC  1125  北京
      */
    //    var rdd1 = sc.textFile("D:/data/spark/scala-word-count-2.txt")
    var rdd1 = sc.textFile("D:\\data\\spark\\scala-word-count-2.txt")
    /**
      * 抽样
      * withReplacement: Boolean true有返回的抽样
      * fraction: Double 抽样比例
      * seed: Long 抽样算法的起始值
      */
    var sampleRdd = rdd1.sample(true, 0.9, 1)
    var rdd2 = sampleRdd.map(_.split("\t")(1))
    //    var rdd2 = rdd1.map((f) => { f.split("\t")(1) })
    /**
      * 计数为1,并转成二元组
      * rdd3是KV格式的RDD
      * K:schoolname V:1
      */
    var rdd3 = rdd2.map((f) => {
      //      println("rdd3-", f)
      (f, 1)
    })
    /**
      * 累加，统计每个产品出现次数
      */
    //    var rdd4 = rdd3.reduceByKey((x: Int, y: Int) => { x + y })
    //    println("rdd3", rdd3)
    var rdd4 = rdd3.reduceByKey((x: Int, y: Int) => {
      //      println("rdd4-", x, y)
      x + y
    })
    /**
      * rdd4中为KV格式元素，将KV位置调换
      * (schoolName,count)
      */
    var rdd5 = rdd4.map((x) => {
      println("rdd5-", x._1, x._2)
      //  多种方式可转换位置
      //      (x._2, x._1)
      x.swap
    })
    /**
      * 对rdd5中的count排序
      * asc true,desc false
      * sortByKey:Transformation类算子
      * 返回 (count,schoolname)
      */
    var rdd6 = rdd5.sortByKey(false)
    /**
      * 取rdd6的前N个元素，
      * Array[(Int,String)]
      */
    var arr = rdd6.take(5)

    val schoolName = arr(0)._2
    var cou = arr(0)._1

    for (obj: (Int, String) <- arr) {
      println("spark word count遍历:", obj._2, obj._1)
    }
    println("spark word count文件版:", schoolName, cou)

    //    过滤，输出到文件
    //    var rsltRdd = rdd6.filter((f: (Int, String)) => {
    //      "HMLC".equals(f._2)
    //    })
    //    var rsltRdd = rdd1.filter((f: String) => {
    //      "HMLC" equals f
    //    })
    var rsltRdd = rdd1.filter(f => {
      !"HMLC".equals(f.split("\t")(1))
    })
    //    var rsltRdd =  rdd6.filter(StringUtils isNotBlank _._2)
    //    val rsltRdd = rdd1.filter((f) => {
    //      //      "".equals(f.split("\t"))
    //      true
    //    })
    //    rsltRdd.saveAsTextFile("sxtFileCount")
    //    避免FileAlreadyExistsException
    rsltRdd.saveAsTextFile("D:\\data\\spark\\scala-word-count-2-" + System.currentTimeMillis)
    //    关闭资源
    sc.stop()
  }

}

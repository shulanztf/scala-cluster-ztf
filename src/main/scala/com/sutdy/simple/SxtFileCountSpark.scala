package com.sutdy.simple

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class SxtFileCountSpark {
}

object SxtFileCountSpark {

  def main(args: Array[String]): Unit = {
    //创建Spark运行时的配置对象，在配置对象里可以设置APP Name,集群URL及运行时的各种资源需求
    val sparkConf = new SparkConf().setAppName("MapOperator")
    //    .setMaster("local")
    //创建sparkContext上下文环境，通过 传入配置对象，实例化一个SparkContext
    val sc = new SparkContext(sparkConf)

    /**
     *     读取文件,数据格式
     * 2017-01-01  HHCF  1323  青岛
     * 2017-01-01  HMLC  1125  北京
     */
    var rdd1 = sc.textFile("hdfs://hserver131:9000/data/log.txt")
    var rdd2 = rdd1.map((f) => { f.split("\t")(1) })
    /**
     * 计数为1,并转成二元组
     * rdd3是KV格式的RDD
     * K:schoolname V:1
     */
    var rdd3 = rdd2.map((f) => { (f, 1) })
    /**
     *  累加，统计每个产品出现次数
     */
    var rdd4 = rdd3.reduceByKey((x: Int, y: Int) => { x + y })
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
    for (obj: (Int, String) <- arr) {
      println("spark word count遍历:", obj._2, obj._1)
    }
    println("spark word count文件版:", arr(0)._2, arr(0)._1)

    //    输出到文件
    var rsltRdd = rdd6.filter((f: (Int, String)) => { true })
    //    避免FileAlreadyExistsException
    rsltRdd.saveAsTextFile("hdfs://hserver131:9000/data/result/hhcf-" + System.currentTimeMillis)
    //
    sc.stop()
  }
}

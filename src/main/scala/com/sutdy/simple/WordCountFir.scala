package com.sutdy.simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 统计字符出现次数 http://blog.csdn.net/bluejoe2000/article/details/41556979
 */
class WordCountFir {

}

object WordCountFir {
  
  def main(args: Array[String]) {
    println("aaa")
    if (args.length < 1) {
      System.err.println("无文件: <file>")
//      TODO 无文件时，案例退出
//      System.exit(1)
    }
    val file1 = "D:\\data\\spark\\scala-word-count-1.txt";
    

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val line = sc.textFile(file1)
    //    分割字符，组装成map<key,1>
    val map = line.flatMap((file: String) => { file.split(" ") }).map((f: String) => { (f, 1) })
    //    .reduceByKey(func=>{fun})
    // 累加,返回 RDD[(String, Int)]
    val rdd1 = map.reduceByKey((n1: Int, n2: Int) => { n1 + n2 })
    //    Array[(String, Int)]
    val arr1 = rdd1.collect()
    //
    arr1.foreach(println)
    // 结束
    sc.stop()
  }

}

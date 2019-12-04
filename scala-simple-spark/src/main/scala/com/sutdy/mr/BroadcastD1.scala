package com.sutdy.mr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Description: https://www.2cto.com/net/201711/701759.html Spark2.2广播变量broadcast原理及源码分析（代码实例）
  * Auther: zhaotf
  * Date: 2018/11/15 0015 11:24
  */
object BroadcastD1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BroadcastD1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val factor = List[Int](1, 2, 3)
    val factorBroadcast = sc.broadcast(factor)
    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numRDD = sc.parallelize(nums, 3)

    val list = new ListBuffer[List[Int]]()
    val resRDD = numRDD.mapPartitions(f => {
      while (f.hasNext) {
        list += f.next() :: (factorBroadcast.value) // (两个冒号)表示普通元素与List的连接操作,(三个冒号)表示List的连接操作
      }
      list.iterator
    })
    resRDD.foreach(println)
  }

}

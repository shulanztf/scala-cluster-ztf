package com.sutdy.simple

import org.apache.spark.{SparkConf, SparkContext}
import scala.math.random

/**
  * 利用spark进行圆周率的计算
  *
  * @see http://blog.csdn.net/sinat_31726559/article/details/51625071
  */
class SparkPai {

}

object SparkPai {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkPai").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //    分片数
    val slices = if (args.length > 0) args(0).toInt else 2
    //    为避免溢出，n不超过int的最大值
    val n = math.min(1000000L * slices, Int.MaxValue).toInt
    println("aaa:", n)
    //计数
    val rdd1 = sc.parallelize(1 until n, slices).map(f => {
      //      小于1的随机数
      val x = random * 2 - 1
      val y = random * 2 - 1
//      println("bbb:",random, x, y, x * x + y * y)
      //      点到圆心的的值，小于1计数一次，超出1就不计算
      if (x * x + y * y < 1) 1 else 0
    })

    //汇总累加落入的圆中的次数
    val count = rdd1.reduce(_ + _)
    //    val count = rdd1.reduce((x, y) => {
    //      x + y
    //    })
    println("scala圆周率:", count, n, 4.0 * count / n)

    //
    sc.stop()
  }

}

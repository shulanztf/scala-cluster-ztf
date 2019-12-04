package com.sutdy.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/50484025 Hadoop经典案例Spark实现（六）——求最大的K个值并排序
  */
object mapreduceD6 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hhcf-sparkPai4").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fourth = sc.textFile("/data/spark/source/six", 3) // 读取文件，并，设置并行数量
    var idx = 0;
    fourth.filter(f => {
      // 过滤无效数据
      f.length > 0 && f.split(",").length == 4
    }).map(line => {
      // 取出金额列
      line.split(",")(2)
    }).map(line => {
      // 字符串转换成数字,并,转成二元组,金额当KEY
      (line.trim.toInt, "")
    }).sortByKey(false) // 降序
      .take(5) // 取TOP5
      .foreach(f => {
      idx += 1
      println("排名前5:", idx, f._1)
    })

  }

}

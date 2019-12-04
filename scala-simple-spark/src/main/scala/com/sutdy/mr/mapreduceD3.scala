package com.sutdy.mr

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/50475788 Hadoop经典案例Spark实现（三）——数据排序
  */
object mapreduceD3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hhcf-sparkPai1").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val fourth = sc.textFile("/data/spark/source/three", 3) // 读取文件，并，设置并行数量
    var idx = 0
    fourth.filter(f => {
      // 过滤无数数据
      f.trim().length > 0
    }).map(num => {
      // 转成二元组，内容当K，准备排序
      (num.trim.toInt, "")
    }).partitionBy(new HashPartitioner(1))
      // 由入输入文件有多个，产生不同的分区，为了生产序号，使用HashPartitioner将中间的RDD归约到一起。
      .sortByKey().map(t => {
      idx += 1
      println("aa:", idx, t._1)
      (idx, t._1)
    }).collect().foreach(x => { // collect()算子将数据从RDD中提取出来，将远程数据通过网络传输到本地，如果数据量特别大的话，会造成很大的网络压力，更为严重的问题是会造成driver端的内存溢出。
      //    }).foreach(x => {
      println(x._1, "\t", x._2)
    }
    )

  }

}

package com.sutdy.mr

import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/50482948 Hadoop经典案例Spark实现（四）——平均成绩
  */
class  mapreduceD4 {
}


object mapreduceD4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hhcf-sparkPai4").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fourth = sc.textFile("/data/spark/source", 3)// 读取文件，并，设置并行数量

    val res = fourth.filter(_.trim.length>0).map(line=>(line.split("\t")(0).trim,line.split("\t")(1).trim.toInt)).groupByKey.map(x=>{
      var num = 0.0
      var sum = 0
      for(i <- x._2) {
        sum = sum + i
        num = num + 1
      }
      val avg = sum/num
      val format = f"$avg%1.2f".toDouble
      println("aa:",x._1,format)
      (x._1,format)
    }).collect().foreach(x => println(x._1 + "\t" + x._2))
    //    fourth.filter((s:String)=>{s.trim().length>0}) filter全代码示例，返回boolean
    //    fourth.filter(_.trim.length>0).map((line:String)=>{(line,3)}) map全代码不便，返回二元组
//    sc.tra
  }
}


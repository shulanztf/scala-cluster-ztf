package com.sutdy.mr

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description:https://blog.csdn.net/liu16659/article/details/79902482 Spark案例实战之一
  * 一.计算最受欢迎的老师
  * Auther: zhaotf
  * Date: 2018/11/15 0015 13:31
  */
object PopularTeacher {
  def main(args: Array[String]): Unit = {
    val words = Array("http://bigdata.xiaoniu.com/laozhao",
      "http://bigdata.xiaoniu.com/laozhao",
      "http://bigdata.xiaoniu.com/laozhao",
      "http://bigdata.xiaoniu.com/laozhao",
      "http://bigdata.xiaoniu.com/laozhao",
      "http://java.xiaoniu.com/laozhang",
      "http://java.xiaoniu.com/laozhang",
      "http://python.xiaoniu.com/laoqian",
      "http://java.xiaoniu.com/laoli",
      "http://python.xiaoniu.com/laoli",
      "http://python.xiaoniu.com/laoli")

    val conf = new SparkConf().setAppName("Popular").setMaster("local")
    val sc = new SparkContext(conf)

    //读取数据
    //val result1 :RDD [String]= sc.textFile(args(0))
    val result1 = sc.parallelize(words,2)
    val subjectAndTeacher: RDD[(String, String)] = result1.map(lines => {
      val url = new URL(lines)
      println("url = " + url)
      val host = new URL(lines).getHost
      println("host = " + host)

      val subject = host.substring(0, host.indexOf("."))
      //切分字符串
      val teacher = url.getPath.substring(1) //获得老师的名字
      (subject, teacher) //这是一个直接返回的
    }) //整理数据

    //总的排序
    val result2 = subjectAndTeacher.map(x => (x, 1)) //形成  ((键值对)，1) 这种map
    val result22 = result2.reduceByKey(_ + _) //根据键将相同的合并
    //print("result22's content are:") //并行的程序，你永远都不知道是不是按照程序的顺序输出
    result22.foreach(println)

//    val result3: Array[((String, String), Int)] = result22.collect()
    //println(result3.toBuffer)

    //每个学科里面做排序   局部排序   按照学科的名字排序
    //val result4  = result22.groupBy(_._1._1)
    val result4: RDD[(String, Iterable[((String, String), Int)])] = result22.groupBy(x => x._1._1)

    //二次排序
    //将keys和values转换成List类型，然后按照values排序，然后倒叙输出，然后取前三
    val result5: RDD[(String, List[((String, String), Int)])] = result4.mapValues(_.toList.sortBy(_._2).reverse.take(3))
//    val result = result5.collect()
    result5.foreach(println)
  }
}

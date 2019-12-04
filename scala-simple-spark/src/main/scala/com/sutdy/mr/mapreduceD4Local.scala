package com.sutdy.mr

import org.apache.hadoop.io.file.tfile.Utils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * https://blog.csdn.net/kwu_ganymede/article/details/50482948 Hadoop经典案例Spark实现（四）——平均成绩
  * http://www.cnblogs.com/bonnienote/p/6139671.html Spark Scala 读取GBK文件的方法
  * 自己实现
  */
class mapreduceD4Local {

}

object mapreduceD4Local {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("spark-encoding-test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

//    sc.textFile("/data/spark/source").foreach(x => println(x))
//    transfer(sc, "/data/spark/source").foreach(x => println("GBK:",x))
    transfer(sc,"/data/spark/source").saveAsTextFile("/tmp/spark/encoding-test1")//文本形式保存


//    sc.hadoopFile("/data/spark/source",classOf[TextInputFormat],classOf[LongWritable],classOf[Text]).map(
//      pair => new String(pair._2.getBytes,0,pair._2.getLength,"GBK")
//    ).foreach(str => {
//      println("GBK-"+str)
//    })
  }

  /**
    * 编码转换
    * @param sc
    * @param path
    * @return
    */
  def transfer(sc:SparkContext,path:String):RDD[String] = {
    var rdd1 = sc.hadoopFile(path,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],1);
    rdd1.map(p => {
      new String(p._2.getBytes,0,p._2.getLength,"GBK")
    })
  }
























}

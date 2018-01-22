package com.sutdy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @see http://www.cnblogs.com/shishanyuan/p/4747749.html
  */
class FileWordCount {

}

object FileWordCount {

  def main(args: Array[String]): Unit = {
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sparkCnf = SparkSession.builder().appName("FileWordCount").master("local[2]").getOrCreate().sparkContext
    //创建Streaming的上下文，包括Spark的配置和时间间隔，这里时间为间隔20秒
    val ssc = new StreamingContext(sparkCnf, Seconds(5))
    //    指定监控的目录，在这里为/home/hadoop/temp/
    println("aaa", System.currentTimeMillis())
    val lines = ssc.textFileStream("D:\\data\\spark\\stream\\spark-streaming-text.txt")
    println("bbb", System.currentTimeMillis())
    //    对指定文件夹变化的数据进行单词统计并且打印
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCount.print()
    //    启动Streaming
    ssc.start()
    ssc.awaitTermination()
  }

}


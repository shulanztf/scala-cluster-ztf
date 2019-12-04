package com.sutdy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
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

/**
  * 1.4 实例3：销售数据统计演示
  * 在该实例中将由4.1流数据模拟器以1秒的频度发送模拟数据（销售数据），Spark Streaming通过Socket接收流数据并每5秒运行一次用来处理接收到数据，处理完毕后打印该时间段内销售数据总和，需要注意的是各处理段时间之间状态并无关系。
  */
object SaleAmount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("请填写启动参数: SaleAmount <hostname> <port> ")
      System.exit(1)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sc = SparkSession.builder().appName("SaleAmount").master("local[2]").getOrCreate().sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.map(_.split(",")).filter(_.length == 6)
    val wordCount = words.map(x => (1, x(5).toDouble)).reduceByKey(_ + _)

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
  * 实例4：Stateful演示
  * 该实例为Spark Streaming状态操作，模拟数据由4.1流数据模拟以1秒的频度发送，
  * Spark Streaming通过Socket接收流数据并每5秒运行一次用来处理接收到数据，处理完毕后打印程序启动后单词出现的频度，
  * 相比较前面4.3实例在该实例中各时间段之间状态是相关的。
  */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: StatefulWordCount <filename> <port> ")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    // 定义更新状态方法，参数values为当前批次单词频度，state为以往批次单词频度
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      //      累加器，折叠累加
      //      val currentCount = values.foldLeft(0)((sum, i) => {
      //        sum + i
      //      })
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }

    val conf = SparkSession.builder().appName("StatefulWordCount").master("local[2]").getOrCreate().sparkContext
    // 创建StreamingContext，Spark Steaming运行时间间隔为5秒
    val ssc = new StreamingContext(conf, Seconds(5))
    // 定义checkpoint目录为当前目录
    ssc.checkpoint(".")
    // 获取从Socket发送过来数据
    val lines = ssc.socketTextStream(args(0), args(1).toInt)
    val words = lines.map(_.split(","))
    val wordCount = words.map(x => (x, 1))

    // 使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数
    val stateDstream = wordCount.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

/**
  * Window演示
  * 该实例为Spark Streaming窗口操作，模拟数据由4.1流数据模拟以1秒的频度发送，Spark Streaming通过Socket接收流数据并每10秒运行一次用来处理接收到数据，
  * 处理完毕后打印程序启动后单词出现的频度。相比前面的实例，Spark Streaming窗口统计是通过reduceByKeyAndWindow()方法实现的，
  * 在该方法中需要指定窗口时间长度和滑动时间间隔。
  */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: WindowWorldCount <filename> <port> <windowDuration> <slideDuration>")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = SparkSession.builder().appName("WindowWordCount").master("local[2]").getOrCreate().sparkContext
    val ssc = new StreamingContext(conf, Seconds(5))
    // 定义checkpoint目录为当前目录
    ssc.checkpoint(".")
    // 通过Socket获取数据，该处需要提供Socket的主机名和端口号，数据保存在内存和硬盘中
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(","))
    // windows操作，第一种方式为叠加处理，第二种方式为增量处理
    val wordCount = words.map(x => (x, 1)).reduceByKeyAndWindow((a: Int, b: Int) => {
      (a + b)
    }, Seconds(args(2).toInt), Seconds(args(3).toInt))
//    val wordCount1 = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(args(2).toInt), Seconds(args(3).toInt))

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

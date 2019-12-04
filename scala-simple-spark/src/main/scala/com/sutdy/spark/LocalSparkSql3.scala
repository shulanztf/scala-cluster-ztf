package com.sutdy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * @see http://lxw1234.com/archives/2016/10/772.htm
  */
class LocalSparkSql3 {

}

object LocalSparkSql3 {

  def main(args: Array[String]): Unit = {
    // 屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("StructuredNetworkWordCount").master("local[2]").getOrCreate()
    //    val spark = SparkSession.builder().appName("StructuredNetworkWordCount").getOrCreate()
    import spark.implicits._
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCount = words.groupBy("value").count()
    //Complete Mode：输出最新的完整的结果表数据
        val query = wordCount.writeStream.outputMode("complete").format("console").start()
    //    Append Mode：只输出结果表中本批次新增的数据，其实也就是本批次中的数据；
//    val query = wordCount.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
  }

}

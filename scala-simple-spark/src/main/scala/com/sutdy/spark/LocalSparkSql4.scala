package com.sutdy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * @see http://lxw1234.com/archives/2016/10/772.htm
  */
class LocalSparkSql4 {

}

object LocalSparkSql4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("sparkStreamingLocal").master("local").getOrCreate()
    import spark.implicits._
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
    val words = lines.as[String].flatMap(_.split(" "))
    //    Append Mode：只输出结果表中本批次新增的数据，其实也就是本批次中的数据；
    val query = words.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
  }
}

//object WordBlacklist {
//  @volatile private var instance:Broadcast[Seq[String]] = null
//
//  def getInstance(S)
//
//}

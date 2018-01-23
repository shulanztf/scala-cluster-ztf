package com.sutdy.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkSQL结合SparkStreaming，使用SQL完成实时计算中的数据统计
  *
  * @see http://lxw1234.com/archives/2015/11/552.htm
  */
class DapLogStreaming {

}

object DapLogStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkSession.builder().appName("DapLogStreaming").master("yarn-cluster").getOrCreate().sparkContext
    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    //从Kafka中读取数据，topic为daplog，该topic包含两个分区
    val kafkaStream = KafkaUtils.createStream(ssc, "bj11-65:2181", "group_spark_streaming", Map[String, Int]("da" -> 0, "db" -> 1),
      StorageLevel.MEMORY_AND_DISK_SER).map(x => x._2.split("\\|~\\|", -1))
//    kafkaStream.foreachRDD(r=>{})

    ssc.start()
    ssc.awaitTermination()
  }

}


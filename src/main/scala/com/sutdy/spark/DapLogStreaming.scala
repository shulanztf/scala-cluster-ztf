package com.sutdy.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * SparkSQL结合SparkStreaming，使用SQL完成实时计算中的数据统计
  *
  * @see http://lxw1234.com/archives/2015/11/552.htm
  */
class DapLogStreaming {

}

object  DapLogStreaming {

  def main(args: Array[String]): Unit = {
    val sparkConf = SparkSession.builder().appName("DapLogStreaming").master("yarn-cluster").getOrCreate().sparkContext
    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf,Seconds(60))
    //从Kafka中读取数据，topic为daplog，该topic包含两个分区
//    KafkaUtils.



    ssc.start()
    ssc.awaitTermination()
  }

}


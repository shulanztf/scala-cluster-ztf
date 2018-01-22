package com.sutdy.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._


/**
  *
  */
class LocalSparkSql5 {

}

object LocalSparkSql5 {

  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val conf = SparkSession.builder().appName("").master("").getOrCreate().sparkContext
    val ssc = new StreamingContext(conf, Seconds(1))

    ssc.start()
    ssc.awaitTermination()
  }

}


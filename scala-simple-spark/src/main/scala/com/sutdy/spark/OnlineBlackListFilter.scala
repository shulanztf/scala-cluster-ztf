package com.sutdy.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @see http://blog.csdn.net/erfucun/article/details/52291761
  */
class OnlineBlackListFilter {

}

object OnlineBlackListFilter {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val context = SparkSession.builder().appName("OnlineBlackListFilter").getOrCreate().sparkContext
    val ssc = new StreamingContext(context, Seconds(10))

    // 黑名单数据准备，实际上黑名单一般都是动态的，例如在Redis或者数据库中，黑名单的生成往往有复杂的业务逻辑，具体情况算法不同，但是在Spark Streaming进行处理的时候每次都能够访问完整的信息。
    val blackList = Array(("Spy", true), ("Cheater", true))
    val blackListRDD = ssc.sparkContext.parallelize(blackList)
    val adsClickStream = ssc.socketTextStream(args(0), args(1).toInt)

    /**
      * 此处模拟的广告点击的每条数据的格式为：time、name
      * 此处map操作的结果是name、（time，name）的格式
      */
    val adsClickStreamFormatted = adsClickStream.map(ads => (ads.split(",")(1), ads))
    adsClickStreamFormatted.transform(userClickRDD => {
      // 通过leftOuterJoin操作既保留了左侧用户广告点击内容的RDD的所有内容，
      // 又获得了相应点击内容是否在黑名单中
      val joinedBlackListRDD = userClickRDD.leftOuterJoin(blackListRDD)

      /**
        * 进行filter过滤的时候，其输入元素是一个Tuple：（name,((time,name), boolean)）
        * 其中第一个元素是黑名单的名称，第二元素的第二个元素是进行leftOuterJoin的时候是否存在的值。
        * 如果存在的话，表面当前广告点击是黑名单，需要过滤掉，否则的话是有效点击内容；
        */
      val validClicked = joinedBlackListRDD.filter(joinedItem => {
        if (joinedItem._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      validClicked.map(validClicked => {
        validClicked._2._1
      })
    }).print()

    //    计算后的有效数据一般都会写入Kafka中，下游的计费系统会从kafka中pull到有效数据进行计费
    ssc.start()
    ssc.awaitTermination()
  }
}

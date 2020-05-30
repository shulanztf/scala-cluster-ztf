package com.msb.stream.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @description: flink间歇性，生成水位线.
 * @author: zhaotf
 * @create: 2020-05-30 09:44
 */
object PunctuatedWatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //卡口号、时间戳
    env
      .socketTextStream("hserver133", 8888)
      .map(data => {
        val splits = data.split(" ")
        (splits(1), splits(0).toLong)
      })
      .assignTimestampsAndWatermarks(new myWatermark(3000))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .reduce((v1: (String, Long), v2: (String, Long)) => {
        (v1._1 + "," + v2._1, v1._2 + v2._2)
      }).print()

    env.execute()
  }

  class myWatermark(delay: Long) extends AssignerWithPunctuatedWatermarks[(String, Long)] {
    var maxTimeStamp: Long = _

    override def checkAndGetNextWatermark(elem: (String, Long), extractedTimestamp: Long): Watermark = {
      if ("001".equals(elem._1)) {
        maxTimeStamp = extractedTimestamp.max(maxTimeStamp)
        new Watermark(maxTimeStamp - delay)
      } else {
        null
      }
    }

    override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
      element._2
    }
  }
}

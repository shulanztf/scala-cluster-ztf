package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * @description: flink间歇性水位线/水印生成.
 * @author: zhaotf
 * @create: 2020-05-30 09:31
 */
object PunctuatedWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //timestamp monitorid
    env.socketTextStream("hserver133", 8888)
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[String] {
        var maxTime: Long = _

        //根据条件生成水印
        override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long): Watermark = {
          if ("001".equals(lastElement.split(" ")(1))) {
            new Watermark(maxTime - 3000)
          } else {
            null
          }
        }

        //指定元素的事件时间
        override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
          maxTime = maxTime.max(element.split(" ")(0).toLong)
          element.split(" ")(0).toLong
        }
      }).map(x => {
      val elems = x.split(" ")
      (elems(1), 1)
    }).keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2 + value2._2)
        }
      }).print()
    env.execute()
  }
}

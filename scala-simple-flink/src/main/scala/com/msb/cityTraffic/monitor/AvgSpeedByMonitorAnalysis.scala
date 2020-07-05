package com.msb.cityTraffic.monitor


import com.msb.util.{AvgSpeedInfo, TrafficInfo, WriteDataSink}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * @description: 卡口车辆拥堵监控.
 *               滑动窗口长度5分钟，步长1分钟；平均车速=当前窗口经过所有车辆速度之和/车辆数；数据有延迟问题，最长延迟5秒.
 * @author: zhaotf
 * @create: 2020-07-02 21:21
 */
object AvgSpeedByMonitorAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    val stream: DataStream[TrafficInfo] = streamEnv.socketTextStream("hserver133", 9999)
      .map(line => {
        val arr = line.split(",")
        TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficInfo](Time.seconds(5)) { //引入Watermark，并且延迟时间为5秒
      override def extractTimestamp(element: TrafficInfo): Long = element.actionTime
    })

    stream.keyBy(_.monitorId)
      .timeWindow(Time.minutes(5), Time.minutes(1))
      .aggregate( //设计一个累加器：二元组(车速之后，车辆的数量)
        new AggregateFunction[TrafficInfo, (Double, Long), (Double, Long)] {
          override def createAccumulator(): (Double, Long) = (0, 0)

          override def add(value: TrafficInfo, accumulator: (Double, Long)): (Double, Long) = {
            (accumulator._1 + value.speed, accumulator._2 + 1)
          }

          override def getResult(accumulator: (Double, Long)): (Double, Long) = accumulator

          override def merge(a: (Double, Long), b: (Double, Long)): (Double, Long) = {
            (a._1 + b._1, a._2 + b._2)
          }
        },
        (key: String, w: TimeWindow, input: Iterable[(Double, Long)], out: Collector[AvgSpeedInfo]) => {
          val acc: (Double, Long) = input.last
          val avg = (acc._1 / acc._2).formatted("%.2f").toDouble
          out.collect(AvgSpeedInfo(w.getStart, w.getEnd, key, avg, acc._2.toInt))
        }
      ).addSink(new WriteDataSink[AvgSpeedInfo](classOf[AvgSpeedInfo]))
    streamEnv.execute()
  }
}

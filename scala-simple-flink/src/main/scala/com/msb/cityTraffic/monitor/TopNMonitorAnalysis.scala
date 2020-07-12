package com.msb.cityTraffic.monitor

import java.util.Properties

import com.msb.util.{AvgSpeedInfo, TrafficInfo}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
 * @description: 申通卡口统计.
 *               找出平均车速最高的前N个.
 * @author: zhaotf
 * @create: 2020-07-02 21:29
 */
object TopNMonitorAnalysis {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(1)

    //    val props = new Properties()
    //    props.setProperty("bootstrap.servers", "hadoop101:9092,hadoop102:9092,hadoop103:9092")
    //    props.setProperty("group.id", "msb_001")

    //创建一个Kafka的Source
    //    val stream: DataStream[TrafficInfo] = streamEnv.addSource(
    //      new FlinkKafkaConsumer[String]("t_traffic_msb", new SimpleStringSchema(), props).setStartFromEarliest() //从第一行开始读取数据
    //    )
    val stream: DataStream[TrafficInfo] = streamEnv.socketTextStream("hserver133", 9999)
      .filter(StringUtils.isNotBlank(_))
      .map(line => {
        val arr = line.split(",")
        TrafficInfo(arr(0).toLong, arr(1), arr(2), arr(3), arr(4).toDouble, arr(5), arr(6))
      }) //引入Watermark，并且延迟时间为5秒
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TrafficInfo](Time.seconds(5)) {
        override def extractTimestamp(element: TrafficInfo): Long = element.actionTime
      })

    stream.keyBy(_.monitorId)
      //      .timeWindow(Time.seconds(30), Time.seconds(5)) //第一个窗口，负责把每个卡口经过的车辆数量和平均速度计算处理
      .timeWindow(Time.minutes(5), Time.minutes(1)) //第一个窗口，负责把每个卡口经过的车辆数量和平均速度计算处理
      .aggregate( //设计一个累加器：二元组(车速之后，车辆的数量)
        new AggregateFunction[TrafficInfo, (Double, Long), (Double, Long)] {
          override def createAccumulator(): (Double, Long) = (0, 0)

          override def add(value: TrafficInfo, acc: (Double, Long)): (Double, Long) = {
            (acc._1 + value.speed, acc._2 + 1)
          }

          override def getResult(acc: (Double, Long)): (Double, Long) = acc

          override def merge(a: (Double, Long), b: (Double, Long)): (Double, Long) = {
            (a._1 + b._1, a._2 + b._2)
          }
        },
        (k: String, w: TimeWindow, input: Iterable[(Double, Long)], out: Collector[AvgSpeedInfo]) => {
          val acc: (Double, Long) = input.last
          val avg: Double = (acc._1 / acc._2).formatted("%.2f").toDouble
          out.collect(AvgSpeedInfo(w.getStart, w.getEnd, k, avg, acc._2.toInt))
        }
      )
      .assignAscendingTimestamps(_.end)
      //      .timeWindowAll(Time.seconds(1)) //窗口2 ，负责对窗口1输出的数据进行排序取TopN
      .timeWindowAll(Time.minutes(1)) //窗口2 ，负责对窗口1输出的数据进行排序取TopN
      .apply(new AllWindowFunction[AvgSpeedInfo, String, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[AvgSpeedInfo], out: Collector[String]): Unit = {
          //map集合，保存每个卡口的车辆数量,有可能在窗口1多次触发的时候（AllowedLateness），同一个卡口会有多条数据，留下车辆数量最多的
          //            val map: Seq[(String, AvgSpeedInfo)] => mutable.Map[String, AvgSpeedInfo] = scala.collection.mutable.Map[String,AvgSpeedInfo]
          //当前处理数据迟到，采用的是Watermark，不需要map去重
          val list = input.toList.sortBy(_.carCount)(Ordering.Int.reverse).take(3) //降序排序取Top3
          val sb = new mutable.StringBuilder()
          sb.append(s"在窗口${list(0).start} ----${list(0).end} 时间范围内:")
          sb.append("整个城市最拥堵的前3个卡口是:")
          list.foreach(t => {
            sb.append(s"卡口:${t.monitorId},经过的车辆数量为:${t.carCount},平均车速为:${t.avgSpeed}")
          })
          sb.append("\n")
          out.collect(sb.toString())
        }
      })
      .print()
    //      .addSink(new WriteDataSink[AvgSpeedInfo](classOf[AvgSpeedInfo]))

    streamEnv.execute()
  }

}

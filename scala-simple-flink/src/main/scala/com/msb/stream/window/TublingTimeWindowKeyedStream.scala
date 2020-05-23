package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @description: TODO.
 * @author: zhaotf
 * @create: 2020-05-23 17:21
 */
object TublingTimeWindowKeyedStream {

  /**
   * * 1、每隔10s 计算最近Ns数据的wordcount
   * * 2、将每个窗口的计算结果写入到mysql中
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val initStream:DataStream[String] = env.socketTextStream("hserver133",8888)
    val wordStream:DataStream[String] = initStream.flatMap(_.split(" "))
    val pairSteam:DataStream[(String,Int)] = wordStream.map((_,1))
    // 是一个已经分好流的无界流
    val keyByStream:KeyedStream[(String,Int),String] = pairSteam.keyBy(_._1)
    val result:DataStream[(String,Int)] = keyByStream.timeWindow(Time.seconds(5))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = {
          (t._1,t._2+t1._2)
        }
      },new ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          var count = 0
          elements.foreach(row => {
            count = count + row._2
          })
          out.collect((key, count))
        }
      })
    result.print()

    env.execute()
  }

}

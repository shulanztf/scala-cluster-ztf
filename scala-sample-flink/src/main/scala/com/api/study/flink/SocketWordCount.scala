package com.sutdy.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * https://blog.csdn.net/qq_21383435/article/details/84951374 Flink的Socket案例
  */
class SocketWordCount {

}

object SocketWordCount {

//  nc命令准备
//  $ ./nc64.exe  -l -p 9000
//  lose fewf
//    few ife
//    fwew id


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text:DataStream[String] = env.socketTextStream("127.0.0.1",9000)

    import org.apache.flink.api.scala._
    text.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(4),Time.seconds(2))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("SocketWordCount")
  }

}

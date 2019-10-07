package com.sutdy.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * flink-scala实时流处理
  */
class StreamingWCScalaApp {

}

object StreamingWCScalaApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val text = env.socketTextStream("", 9999)
    text.flatMap(_.split(",")).map((_, 1))
      .keyBy(0)
          .timeWindow(Time.seconds(2)).sum(1).print().setParallelism(1)

    env.execute("StreamingWCScalaApp")
  }

}

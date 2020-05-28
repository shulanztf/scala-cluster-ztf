package com.msb.stream.conntable

import java.util.{Timer, TimerTask}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import scala.collection.mutable
import scala.io.Source

/**
 * @description: flink流表join,定期读取数据源文件.
 * @author: zhaotf
 * @create: 2020-05-28 20:55
 */
object TimerQueryTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.socketTextStream("hserver133",8888)

    stream.map(new RichMapFunction[String,String] {

      private val map = new mutable.HashMap[String,String]()

      override def open(parameters: Configuration): Unit = {
        println("init data ...")
        query()
        val timer = new Timer(true)
        timer.schedule(new TimerTask {
          override def run(): Unit = {
            query()
          }
          //1s后，每隔2s执行一次
        },1000,2000)
      }

      def query()={
        val source = Source.fromFile("/data/flink/id2city","UTF-8")
        val iterator = source.getLines()
        for (elem <- iterator) {
          val vs = elem.split(" ")
          map.put(vs(0),vs(1))
        }
      }

      override def map(key: String): String = {
        map.getOrElse(key,"not found city")
      }
    }).print()

    env.execute()

  }
}

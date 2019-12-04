package com.sutdy.flink

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer


/**
  * @ClassName: BatchDemoBroadcastScala
  * @Author: zhaotf
  * @Description:
  * @Date: 2019/12/2 0002 
  */
class BatchDemoBroadcastScala {

}

/**
  * https://www.cnblogs.com/linkmust/p/10901731.html 初识Flink广播变量broadcast
  */
object BatchDemoBroadcastScala {
  val logger: Logger = LoggerFactory.getLogger(BatchDemoBroadcastScala.getClass)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //1: 准备需要广播的数据
    val broadData = ListBuffer[(String, Int)]()
    broadData.append(("abc", 18))
    broadData.append(("def", 29))
    broadData.append(("ghk", 15))
    //1.1处理需要广播的数据
    val toBroadcastData: DataSet[Map[String, Int]] = env.fromCollection(broadData).map(tup => {
      Map(tup._1 -> tup._2)
    }) //待广播数据

    val text = env.fromElements("abc", "def", "ghk", "ztf", "abc")
    val result: DataSet[String] = text.map(mapper = new RichMapFunction[String, String] {
      var allMap = Map[String, Int]()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val it:java.util.Iterator[Map[String, Int]] = getRuntimeContext.getBroadcastVariable[Map[String, Int]]("broadcastMapName").iterator()
        while (it.hasNext) {
          allMap = allMap.++(it.next())
        }
      }

      override def map(value: String): String = {
        //        logger.info("map grams:"+value+","+allMap.get(value))
        if (allMap.get(value) == None) {
          return value + "," + 0
        }
        value + "," + allMap.get(value).get
      }
    }).withBroadcastSet(toBroadcastData, "broadcastMapName")

    result.print()

    //    env.execute("BatchDemoBroadcastScala")
  }

}
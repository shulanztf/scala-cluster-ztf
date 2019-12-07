package com.api.study.flink.stream

import java.lang
import java.text.SimpleDateFormat

import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.slf4j.LoggerFactory
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.CoGroupedStreams.WithWindow
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.util.Collector

/**
  * https://blog.csdn.net/aA518189/article/details/84032660 flink实战--双流join之Join和coGroup的区别和应用
  *
  * @author 赵腾飞
  * @date 2019/12/7/007 17:32
  */
object JoinDemo {
  val logger = LoggerFactory.getLogger(JoinDemo.getClass)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //获取接口传送的数据
    val dataStream1:DataStream[String] = env.readTextFile("")
    val dataStream2:DataStream[String] = env.readTextFile("")
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    //使用样例类StockTransaction封装获取的数据
    val dataStreamMap1 = dataStream1.map(f => {
      val tokens1:Array[String] = StringUtils.split(f,",")
      StockTransaction(tokens1(0),tokens1(1),tokens1(2).toDouble)
    }).assignAscendingTimestamps(f =>{
      format.parse(f.tx_time).getTime
    })
    //使用样例类StockSnapshot封装获取的数据
    val dataStreamMap2 = dataStream2.map(f =>{
      val tokens2 = StringUtils.splitPreserveAllTokens(f,",")
      StockSnapshot(tokens2(0),tokens2(1),tokens2(2).toDouble)
    }).assignAscendingTimestamps(f=>{
      format.parse(f.md_time).getTime
    })

//    进行双流join
//    限定范围是：3秒钟的Event time时间窗口
    val joinStream = dataStreamMap1.coGroup(dataStreamMap2)
      .where(_.tx_code).equalTo(_.md_code)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    val innerJoinStream = joinStream.apply(new InnerJoinFunction)

  }


}

//定义样例类封装接收的数据
case class StockTransaction(tx_time: String, tx_code: String, tx_value: Double)

case class StockSnapshot(md_time: String, md_code: String, md_value: Double)

class InnerJoinFunction extends CoGroupFunction[StockTransaction,StockSnapshot,(String,String,String,Double,Double,String)]{
  override def coGroup(T1: java.lang.Iterable[StockTransaction], T2: java.lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double, Double, String)]): Unit = {
    /**
      * 将Java中的Iterable对象转换为Scala的Iterable
      * scala的集合操作效率高，简洁
      */
    import scala.collection.JavaConverters._
    val scalaT1 = T1.asScala.toList
    val scalaT2 = T2.asScala.toList

//    Inner Join要比较的是同一个key下，同一个时间窗口内的数据
    if(scalaT1.nonEmpty && scalaT2.nonEmpty) {
      for(transaction <- scalaT1) {
        for(snapshot <- scalaT2){
          out.collect(transaction.tx_code,transaction.tx_time, snapshot.md_time,transaction.tx_value,snapshot.md_value,"Inner Join Test")
        }
      }
    }
  }
}
class LeftJoinFunction extends CoGroupFunction[StockTransaction,StockSnapshot,(String,String,String,Double,Double,String)] {
  override def coGroup(T1: lang.Iterable[StockTransaction], T2: lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double, Double, String)]): Unit = {
    /**
      * 将Java中的Iterable对象转换为Scala的Iterable
      * scala的集合操作效率高，简洁
      */
    import scala.collection.JavaConverters._
    val scalaT1 = T1.asScala.toList
    val scalaT2 = T2.asScala.toList
    /**
      * Left Join要比较的是同一个key下，同一个时间窗口内的数据
      */
    if(scalaT1.nonEmpty && scalaT2.isEmpty){
      for(transaction <- scalaT1){
        out.collect(transaction.tx_code,transaction.tx_time, "",transaction.tx_value,0,"Left Join Test")
      }
    }
  }
}
class RightJoinFunction extends CoGroupFunction[StockTransaction,StockSnapshot,(String,String,String,Double,Double,String)] {
  override def coGroup(T1: lang.Iterable[StockTransaction], T2: lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double, Double, String)]): Unit = {
    /**
      * 将Java中的Iterable对象转换为Scala的Iterable
      * scala的集合操作效率高，简洁
      */
    import scala.collection.JavaConverters._
    val scalaT1 = T1.asScala.toList
    val scalaT2 = T2.asScala.toList
    /**
      * Right Join要比较的是同一个key下，同一个时间窗口内的数据
      */
    if(scalaT1.isEmpty && scalaT2.nonEmpty){
      for(snapshot <- scalaT2){
        out.collect(snapshot.md_code, "",snapshot.md_time,0,snapshot.md_value,"Right Join Test")
      }
    }
  }
}

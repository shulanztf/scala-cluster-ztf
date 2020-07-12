package com.msb.util
import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
/**
 * @description: TODO.
 * @author: zhaotf
 * @create: 2020-07-11 20:58
 */
class StreamingTest {

}

object StreamingTest {
  def main(args: Array[String]): Unit = {
//    val kafkaProps = new Properties()
//    //kafka的一些属性
//    kafkaProps.setProperty("bootstrap.servers", "bigdata01:9092")
//    //所在的消费组
//    kafkaProps.setProperty("group.id", "group2")
    //获取当前的执行环境
//    val evn = StreamExecutionEnvironment.getExecutionEnvironment
    //evn.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //kafka的consumer，test1是要消费的topic
//    val kafkaSource = new FlinkKafkaConsumer[String]("test1",new SimpleStringSchema,kafkaProps)
//    //kafkaSource.assignTimestampsAndWatermarks(assigner)
//    //设置从最新的offset开始消费
//    //kafkaSource.setStartFromGroupOffsets()
//    kafkaSource.setStartFromLatest()
//    //自动提交offset
//    kafkaSource.setCommitOffsetsOnCheckpoints(true)
//
//    //flink的checkpoint的时间间隔
//    //evn.enableCheckpointing(2000)
//    //添加consumer
//    val stream = evn.addSource(kafkaSource)
//    evn.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE)
    //stream.setParallelism(3)
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

    val text:DataStream[(String,String,Long)] = stream
      .map(x=>{("car1","abc",System.currentTimeMillis())})
    //text.print()
    //启动执行

    text.addSink(new Ssinks())

    streamEnv.execute("kafkawd")

  }
}
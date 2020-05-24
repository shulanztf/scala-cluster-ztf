package com.msb.stream.source

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

/**
 * @description: 数据生成，kafka端.
 * @author: zhaotf
 * @create: 2020-05-24 16:18
 */
object FlinkKafkaProducer {

  def main(args: Array[String]): Unit = {
    //配置连接kafka的信息  Properties
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hserver133:9092")
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    //创建一个kafka生产者对象
    val producer = new KafkaProducer[String,String](properties)

    //不要轻易调用getLines
    val iterator:Iterator[String] = Source.fromFile("/data/flink/carFlow_all_column_test.txt").getLines()
    for (i <- 1 to 100) {
      for (elem <- iterator) {
        println(elem)
        //kv mq
        val splits = elem.split(",")
        val monitorId = splits(0).replace("'","")
        val carId = splits(2).replace("'","")
        val timestamp = splits(4).replace("'","")
        val speed = splits(6)
        val builder = new StringBuilder
        val info = builder.append(monitorId + "\t").append(carId + "\t").append(timestamp + "\t").append(speed)
        producer.send(new ProducerRecord[String,String]("flink-kafka-1",info.toString))
        Thread.sleep(1000)
      }
    }
  }

}

package com.sutdy.hlht

import java.util.Properties

import com.sutdy.hlht.ReadingFromKafka.{KAFKA_BROKER, TOPIC, TRANSACTION_GROUP, ZOOKEEPER_HOST}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.common.serialization.ByteArraySerializer

/**
  * @author 赵腾飞
  * @date 2019/10/29/029 14:48
  * https://blog.csdn.net/wugenqiang/article/details/81738939 kafka连接flink流计算,实现flink消费kafka的数据
  */
class ReadingFromKafka {

}

/**
  * scala+flink+kafka
  */
object ReadingFromKafka {
  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP = "test-consumer-group"
  private val TOPIC = "hmlc-order-1"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 仅执行一次语义

    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    import org.apache.flink.api.scala._
    val transaction = env.addSource(new FlinkKafkaConsumer[String](TOPIC, new SimpleStringSchema(), kafkaProps))
    transaction.print()

    env.execute("flink-kafka-test")
  }
}

object ReadingFromKafkaTomcat {
  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP = "test-consumer-group"
  private val TOPIC = "hmlc-order-1"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 仅执行一次语义

    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    // 整型列表
    val nums: List[Int] = List(1, 2, 9, 10,11)
    var rslt:Map[String,String] = Map()

    import org.apache.flink.api.scala._
    val transaction:DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](TOPIC, new SimpleStringSchema(), kafkaProps))
    var index = 0
    transaction.flatMap(_.split(" ")).map(x =>{
      index = index + 1
      (index,x)
    } ).filter(x => {
      nums.contains(x._1)
    }).map(x=>{
      if(1 == x._1) {
        ("date",x._2)
      }else  if(2==x._1) {
        ("time",x._2)
      }else if(9==x._1) {
        ("client_id",x._2)
      }else  if(10==x._1) {
        ("method",x._2)
      }else  if(11==x._1) {
        ("url",x._2)
      }else {
        ("aa",x)
      }
    }).print()
//    transaction.print()
    env.execute("flink-kafka-tomcat")
  }

}

/**
  * flink消费kafka的tomcat日志
  */
object ReadingFromKafkaTomcat1 {
  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP = "test-consumer-group"
  private val TOPIC = "hmlc-order-1"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 仅执行一次语义

    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    import org.apache.flink.api.scala._
    val transaction:DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](TOPIC, new SimpleStringSchema(), kafkaProps))
    transaction.map(x=> {
      val arr = StringUtils.split(x," ")
      var rslt:Map[String,String] = Map()
      val date:String = arr(0)+" "+arr(1)
      rslt += ("message" -> x)
      rslt += ("date" -> date)
      rslt += ("client_id" -> arr(8))
      rslt += ("method" -> arr(9))
      rslt += ("url" -> arr(10))

      val map:Map[String,String] = getIp(arr(10))
      if(map.nonEmpty) {
        rslt += ("protocol" -> map.get("protocol").get)
        rslt += ("authority" -> map.get("authority").get)
        rslt += ("path" -> map.get("path").get)
      }
      rslt
    }).print()
    env.execute("flink-kafka-tomcat")
  }

  def getIp(url:String): Map[String,String] = {
    var map:Map[String,String] = Map()
    try {
      var url1 = new java.net.URL(url)
      map += ("protocol" -> url1.getProtocol)
      map += ("authority" -> url1.getAuthority)
      map += ("path" -> url1.getPath)
    }catch  {
      case ex:Exception => {
        ex.printStackTrace()
      }
    }
    map
  }

}


/**
  * flink消费kafka的tomcat日志，清洗后，推送kafka
  */
object ReadingFromKafkaTomcat2 {
  private val ZOOKEEPER_HOST = "localhost:2181"
  private val KAFKA_BROKER = "localhost:9092"
  private val TRANSACTION_GROUP = "test-consumer-group"
  private val KAFKA_TOMCAT_TOPIC_NAME = "hmlc-tomcat-to-kafka-1"
  private val KAFKA_ELK_TOPIC_NAME = "hmlc-kafka-to-elk-1"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) // 仅执行一次语义

    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    import org.apache.flink.api.scala._

    // 消费Kafka
    val transaction:DataStream[String] = env.addSource(new FlinkKafkaConsumer[String](KAFKA_TOMCAT_TOPIC_NAME, new SimpleStringSchema(), kafkaProps))

    transaction.map(x=> {
      val arr = StringUtils.split(x," ")
      var rslt:Map[String,String] = Map()
      val date:String = arr(0)+" "+arr(1)
      rslt += ("message" -> x)
      rslt += ("date" -> date)
      rslt += ("client_id" -> arr(8))
      rslt += ("method" -> arr(9))
      rslt += ("url" -> arr(10))

      val map:Map[String,String] = getIp(arr(10))
      if(map.nonEmpty) {
        rslt += ("protocol" -> map.get("protocol").get)
        rslt += ("authority" -> map.get("authority").get)
        rslt += ("path" -> map.get("path").get)
      }
      rslt
    }).print()

    val dr = transaction.map(_.toString)
    dr.print()

    // sind到kafka
    val sinkP = new Properties()
    sinkP.setProperty("bootstrap.servers", "localhost:9092")
    sinkP.setProperty("key.serializer", classOf[ByteArraySerializer].getName)
    sinkP.setProperty("value.serializer", classOf[ByteArraySerializer].getName)
    val sink = new FlinkKafkaProducer[String](KAFKA_ELK_TOPIC_NAME,new SimpleStringSchema(),sinkP)
    dr.addSink(sink)

    env.execute("flink-kafka-tomcat")
  }

  def getIp(url:String): Map[String,String] = {
    var map:Map[String,String] = Map()
    try {
      var url1 = new java.net.URL(url)
      map += ("protocol" -> url1.getProtocol)
      map += ("authority" -> url1.getAuthority)
      map += ("path" -> url1.getPath)
    }catch  {
      case ex:Exception => {
        ex.printStackTrace()
      }
    }
    map
  }

}
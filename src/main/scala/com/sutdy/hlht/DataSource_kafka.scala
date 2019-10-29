package com.sutdy.hlht

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.common.serialization.ByteArraySerializer

/**
  * @author 赵腾飞
  * @date 2019/10/29/029 17:03
  * https://www.cnblogs.com/niutao/p/10548616.html Flink--sink到kafka
  */
class DataSource_kafka {

}

object DataSource_kafka {
  def main(args: Array[String]): Unit = {
    //1指定kafka数据流的相关信息
    val zkCluster = "localhost:2181"
    val kafkaCluster = "localhost:9092"
    val kafkaGroupId = "test-consumer-group"
    val kafkaTopicName = "tomcat-to-kafka-1"
    val sinkTopicName = "kafka-to-elk-1"

    //2.创建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //3.创建kafka数据流
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaCluster)
    properties.setProperty("zookeeper.connect", zkCluster)
    properties.setProperty("group.id", kafkaGroupId)
    val kafka09 = new FlinkKafkaConsumer[String](kafkaTopicName, new SimpleStringSchema(), properties)

    import org.apache.flink.api.scala._

    //4.添加数据源addSource(kafka09)
    val text = env.addSource(kafka09).setParallelism(2)

    /**
      * test#CS#request http://b2c.csair.com/B2C40/query/jaxb/direct/query.ao?t=S&c1=HLN&c2=CTU&d1=2018-07-12&at=2&ct=2&inf=1#CS#POST#CS#application/x-www-form-urlencoded#CS#t=S&json={'adultnum':'1','arrcity':'NAY','childnum':'0','depcity':'KHH','flightdate':'2018-07-12','infantnum':'2'}#CS#http://b2c.csair.com/B2C40/modules/bookingnew/main/flightSelectDirect.html?t=R&c1=LZJ&c2=MZG&d1=2018-07-12&at=1&ct=2&inf=2#CS#123.235.193.25#CS#Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1#CS#2018-01-19T10:45:13:578+08:00#CS#106.86.65.18#CS#cookie
      * */
    val values:DataStream[ProcessedData] = text.map(line => {
        var encrypted = line
        val values = encrypted.split("#CS#")
        val valuesLength = values.length
        var regionalRequest =  if(valuesLength > 1) values(1) else ""
        val requestMethod = if (valuesLength > 2) values(2) else ""
        val contentType = if (valuesLength > 3) values(3) else ""
        //Post提交的数据体
        val requestBody = if (valuesLength > 4) values(4) else ""
        //http_referrer
        val httpReferrer = if (valuesLength > 5) values(5) else ""
        //客户端IP
        val remoteAddr = if (valuesLength > 6) values(6) else ""
        //客户端UA
        val httpUserAgent = if (valuesLength > 7) values(7) else ""
        //服务器时间的ISO8610格式
        val timeIso8601 = if (valuesLength > 8) values(8) else ""
        //服务器地址
        val serverAddr = if (valuesLength > 9) values(9) else ""
        //获取原始信息中的cookie字符串
        val cookiesStr = if (valuesLength > 10) values(10) else ""
        ProcessedData(regionalRequest,
          requestMethod,
          contentType,
          requestBody,
          httpReferrer,
          remoteAddr,
          httpUserAgent,
          timeIso8601,
          serverAddr,
          cookiesStr)
    })
    values.print()
    val remoteAddr:DataStream[String] = values.map(_.remoteAddr)
    remoteAddr.print()

    //TODO sink到kafka
    val sinkP = new Properties()
    sinkP.setProperty("bootstrap.servers", "localhost:9092")
    sinkP.setProperty("key.serializer", classOf[ByteArraySerializer].getName)
    sinkP.setProperty("value.serializer", classOf[ByteArraySerializer].getName)
    val sink = new FlinkKafkaProducer[String](sinkTopicName,new SimpleStringSchema(),sinkP)
    remoteAddr.addSink(sink)

    //5.触发运算
    env.execute("flink-kafka-wordcunt")
  }
}

//保存结构化数据
case class ProcessedData(regionalRequest: String,
                         requestMethod: String,
                         contentType: String,
                         requestBody: String,
                         httpReferrer: String,
                         remoteAddr: String,
                         httpUserAgent: String,
                         timeIso8601: String,
                         serverAddr: String,
                         cookiesStr: String
                        )
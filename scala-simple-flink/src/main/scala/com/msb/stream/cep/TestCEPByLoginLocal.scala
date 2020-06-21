package com.msb.stream.cep

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @description: flink cep编程，非法login检测.
 * @author: zhaotf
 * @create: 2020-06-20 18:30
 */
object TestCEPByLoginLocal {

  //实时的根据用户登录日志，来判断哪些用户是恶意登录
  //恶意登录：10分钟内，连续登录失败3次以上。
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) //并行度
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置数据时间语义
    import org.apache.flink.streaming.api.scala._
    //    1、准备数据流
    val stream: DataStream[LoginEvent] = env.fromCollection(Array(
      LoginEvent(1, "张三", "fail", 1577080469000L), //2019-12-23 05:54:29
      LoginEvent(2, "张三", "fail", 1577080470000L), //2019-12-23 05:54:30
      LoginEvent(3, "张三", "fail", 1577080472000L), //2019-12-23 05:54:32
      LoginEvent(4, "李四", "fail", 1577080469000L), //2019-12-23 05:54:29
      LoginEvent(5, "李四", "success", 1577080473000L), // 2019-12-23 05:54:33
      LoginEvent(6, "张三", "fail", 1577080477000L) //2019-12-23 05:54:37
    )).assignAscendingTimestamps(_.loginTime) //引入时间

    //    2、定义pattern
    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("start") //命名
      .where(_.loginType.equals("fail"))
      .timesOrMore(3) //三次以上
      .greedy //贪婪模式
      .within(Time.seconds(10)) //测试的时候使用10秒替代10分钟

    //    3、检测数据
    val ps: PatternStream[LoginEvent] = CEP.pattern(stream.keyBy(_.userName), pattern)

    //    4、选择数据并且返回
    val result: DataStream[String] = ps.select(
      //patternSelectFun 是一个Map集合，Map中的key是Pattern中定义的事件名字。
      patternSelectFun => {
        val sb = new StringBuilder
        val list: List[LoginEvent] = patternSelectFun.get("start").get.toList
        sb.append("用户名:").append(list(0).userName).append(" 恶意登录，")
        for (i <- 0 until list.size) {
          sb.append(s"第${i + 1}次登录失败的时间是:").append(list(i).loginTime).append(" , ").append(s"id 是${list(i).id};")
        }
        sb.toString()
      }
    )
    result.print()

    env.execute()
  }

  case class LoginEvent(id: Long, userName: String, loginType: String, loginTime: Long)

}

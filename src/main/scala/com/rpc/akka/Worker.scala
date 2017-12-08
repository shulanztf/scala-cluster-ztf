package com.rpc.akka

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import java.util.UUID
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit

/**
 * XN学堂,akka
 */
class Worker(val mHost: String, val mPort: Int, val wMemory: Int, val wCores: Int) extends Actor {
  val WORKER_ID = UUID.randomUUID().toString()

  //  actor构造方法后，receive方法前，执行
  override def preStart(): Unit = {
    import context.dispatcher

    //    连接MASTER，拿到master的一个代理对象
    val master = context.actorSelection(s"akka.tcp://${Master.MASTER_ACTOR_SYSTEM_NAME}@$mHost:$mPort/user/${Master.MASTER_ACTOR_NAME}")
    master ! RegisterMsg(WORKER_ID, wMemory, wCores) //    向master发送注册消息

    //    心跳定时调度
    val initialDelay: FiniteDuration = new FiniteDuration(0, TimeUnit.MILLISECONDS)
    val interval: FiniteDuration = new FiniteDuration(1000, TimeUnit.MILLISECONDS) //间隔一秒
    context.system.scheduler.schedule(initialDelay, interval, this.self, HeartBeatMsg(WORKER_ID, "aa"))
  }

  //    向master发送心跳消息
  def sentHeartBeat() {
    val master = context.actorSelection(s"akka.tcp://${Master.MASTER_ACTOR_SYSTEM_NAME}@$mHost:$mPort/user/${Master.MASTER_ACTOR_NAME}")
    master ! HeartBeatMsg(WORKER_ID, "aa")
  }

  //  receive重复执行
  override def receive: Actor.Receive = {
    case "Response" => {
      println("aaaaaaaaaaaaaaa")
    }
    case HeartBeatMsg(workerId, content) => {
      // 心跳定时发送
      this.sentHeartBeat()
    }
  }

}

object Worker {
  val WORKER_ACTOR_SYSTEM_NAME = "WorkerActorSystem"
  val WORKER_ACTOR_NAME = "Worker"

  def main(args: Array[String]): Unit = {
    val workerHost = args(0) //worder地址
    val workerPort = args(1).toInt //worder端口
    val masterHost = args(2) //master地址
    val masterPort = args(3).toInt //master端口
    val wMemory = args(4).toInt //内存大小
    val wCores = args(5).toInt //内核数

    val configStr = s"""
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.hostname= "$workerHost"
      |akka.remote.netty.tcp.port = "$workerPort"
    """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val actorSystem = ActorSystem(WORKER_ACTOR_SYSTEM_NAME, config)
    actorSystem.actorOf(Props(new Worker(masterHost, masterPort, wMemory, wCores)), WORKER_ACTOR_NAME)

  }

}

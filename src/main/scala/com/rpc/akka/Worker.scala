package com.rpc.akka

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import java.util.UUID

/**
 * XN学堂,akka
 */
class Worker(val mHost: String, val mPort: Int, val wMemory: Int, val wCores: Int) extends Actor {
  val WORKER_ID = UUID.randomUUID().toString()
  //  actor构造方法后，receive方法前，执行
  override def preStart(): Unit = {
    //    连接MASTER，拿到master的一个代理对象
    val master = context.actorSelection(s"akka.tcp://${Master.MASTER_ACTOR_SYSTEM_NAME}@$mHost:$mPort/user/${Master.MASTER_ACTOR_NAME}")
    //    向master发送消息
    master ! RegisterMsg(WORKER_ID, wMemory, wCores)
  }

  //  receive重复执行
  override def receive: Actor.Receive = {
    case "Response" => {
      println("aaaaaaaaaaaaaaa")
    }
  }

}

object Worker {
  val WORKER_ACTOR_SYSTEM_NAME = "WorkerActorSystem"
  val WORKER_ACTOR_NAME = "Worker"

  def main(args: Array[String]): Unit = {
    val workerHost = args(0) //worder地址
    val masterHost = args(1) //master地址
    val masterPort = args(2).toInt //master端口
    val wMemory = args(3).toInt //内存大小
    val wCores = args(4).toInt //内核数

    val configStr = s"""
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.hostname= "$workerHost"
    """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val actorSystem = ActorSystem(WORKER_ACTOR_SYSTEM_NAME, config)
    actorSystem.actorOf(Props(new Worker(masterHost, masterPort, wMemory, wCores)), WORKER_ACTOR_NAME)

  }

}

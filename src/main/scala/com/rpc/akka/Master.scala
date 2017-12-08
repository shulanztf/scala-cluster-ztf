package com.rpc.akka

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * AKKA
 */
class Master extends Actor {
  val CHECK_INTERVAL = 5000
  val id2worker = new mutable.HashMap[String, WorkerInfo]()
  val workers = new mutable.HashSet[WorkerInfo]()

  //  master构造方法后，receive方法前 要启动一个定时器
  override def preStart(): Unit = {
    import context.dispatcher
    val initialDelay: FiniteDuration = new FiniteDuration(0, TimeUnit.MILLISECONDS)
    val interval: FiniteDuration = new FiniteDuration(CHECK_INTERVAL, TimeUnit.MILLISECONDS)
    //    启动定时器
    context.system.scheduler.schedule(initialDelay, interval, this.self, CheckTimeOutWorker)
  }

  //  接收要处理的消息
  override def receive: Receive = {
    case CheckTimeOutWorker => {
      println("master 信息校验", workers.toBuffer)
    }
    case RegisterMsg(id, memory, cores) => {
      val workerInfo = new WorkerInfo(id, memory, cores)
      id2worker(id) = workerInfo
      //      id2worker.put(id, workerInfo)
      //      id2worker += ((id,workerInfo))
      //      id2worker += (id -> workerInfo)
      workers += workerInfo
    }

  }
}

object Master {
  val MASTER_ACTOR_SYSTEM_NAME = "MasterActorSystem"
  val MASTER_ACTOR_NAME = "Master"

  def main(args: Array[String]): Unit = {
    //    val host = "localhost"
    //    val port = 8080
    val host = args(0)
    val port = args(1).toInt
    val configStr = s"""
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = "$port"
    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    //    创建ActorSystem实例
    val actorSystem = ActorSystem(MASTER_ACTOR_SYSTEM_NAME, config)
    //    创建Actor
    val masterActor = actorSystem.actorOf(Props(classOf[Master]), MASTER_ACTOR_NAME)
    //    发送异步消息
    masterActor ! "CheckTimeOut"

  }

}


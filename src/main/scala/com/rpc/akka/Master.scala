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
    //    启动定时器，任务调度
    context.system.scheduler.schedule(initialDelay, interval, self, CheckTimeOutWorker)
  }

  //  接收要处理的消息
  override def receive: Receive = {
    case CheckTimeOutWorker => {
      var current = System.currentTimeMillis()
      val dead = workers.filter(p => current - p.lasterTime > CHECK_INTERVAL) //      过滤出超时worker
      println("过滤出超时worker", dead.toBuffer)
      dead.foreach(workerInfo => {
        id2worker remove workerInfo.workerId
        workers remove workerInfo
      }) //      去除宕机的worker
    }
    case RegisterMsg(id, memory, cores) => {
      val workerInfo = new WorkerInfo(id, memory, cores)
      id2worker(id) = workerInfo
      workers += workerInfo
      println("客户端注册", id)
    }
    case HeartBeatMsg(workerId, content) => {
      workers.filter(_.workerId equals workerId).foreach(_.lasterTime = System.currentTimeMillis())
      println("客户端心跳更新", workerId)
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


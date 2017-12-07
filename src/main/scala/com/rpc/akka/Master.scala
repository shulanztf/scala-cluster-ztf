package com.rpc.akka

import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor

/**
 * AKKA
 */
class Master extends Actor {

  //  接收要处理的消息
  override def receive: Receive = {
    case "Register" => {

    }
    case "HeartBeat" => {

    }
    case "CheckTimeOut" => {
      println("master 信息校验")
    }
  }
}

object Master {

  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 8080
    val configStr = s"""
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.hostname = "$host"
      |akka.remote.netty.tcp.port = "$port"
    """.stripMargin

    val config = ConfigFactory.parseString(configStr)
    //    创建ActorSystem实例
    val actorSystem = ActorSystem("MasterActorSystem", config)
    //    创建Actor
    val masterActor = actorSystem.actorOf(Props(classOf[Master]), "MasterA")
    //    发送异步消息
    masterActor ! "CheckTimeOut"

  }

}


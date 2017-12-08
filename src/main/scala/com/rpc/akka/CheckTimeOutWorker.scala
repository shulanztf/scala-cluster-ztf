package com.rpc.akka
/**
 * master内部消息
 */
class CheckTimeOutWorker {
}
//注册消息
case class RegisterMsg(id: String, memory: Int, cores: Int) extends Serializable
//worker消息
case class WorkerInfo(workerId: String, memory: Int, cores: Int) {
  var lasterTime: Long = System.currentTimeMillis()
}
//心跳消息
case class HeartBeatMsg(workerId: String, content: String) extends Serializable

object CheckTimeOutWorker {
}

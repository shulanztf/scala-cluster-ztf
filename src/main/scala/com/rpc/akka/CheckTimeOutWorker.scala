package com.rpc.akka
/**
 * master内部消息
 */
class CheckTimeOutWorker {
}
//注册消息
case class RegisterMsg(id: String, memory: Int, cores: Int) extends Serializable
//worker信息
case class WorkerInfo(workerId: String, memory: Int, cores: Int)

object CheckTimeOutWorker {

}


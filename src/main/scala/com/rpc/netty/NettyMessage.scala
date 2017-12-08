package com.rpc.netty

/**
 * XN学堂，scala/netty
 */
class NettyMessage {
}

/**
 * 注册消息
 */
case class RegisterMsg(workerId: String, cont: String) extends NettyMessage with Serializable
/**
 * 响应消息
 */
case class ResposeMsg(cont: String) extends NettyMessage with Serializable
/**
 * 心跳消息
 */
case class HeartBeatMsg(workerId: String, content: String) extends NettyMessage with Serializable
/**
 * 响应确认消息，内部用
 */
case class ConfrimMsg() extends NettyMessage

object NettyMessage {

}

package com.rpc.server
/**
 * xn学堂
 */
trait RemoteMsg extends Serializable
//注册消息
case class RegisterMsg(account: String, password: String) extends RemoteMsg
// 心跳消息
case class HeartBeatMsg(clientId: String, content: String) extends RemoteMsg
// 结果消息
case class ResultMsg(rslt: String) extends RemoteMsg

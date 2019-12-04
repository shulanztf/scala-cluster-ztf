package com.rpc.server

import java.io.ObjectInputStream
import java.net.ServerSocket
import java.io.ObjectOutputStream
import java.net.Socket

/**
 * 模拟XN学堂
 */
class RpcServerLocal {
  private def handlerRegister(account: String, password: String): ResultMsg = {
    println(s"注册消息 $account,$password coll")
    ResultMsg("注册消息success")
  }

  private def handlerHeartbeat(id: String, content: String): ResultMsg = {
    println(s"心跳消息 $id,$content coll")
    ResultMsg("心跳success")
  }

  def bind(port: Int): Unit = {
    //    创建ServerSocket
    val ss = new ServerSocket(port)
    val cs: Socket = ss.accept()
    //    读取输入流
    val ois: ObjectInputStream = new ObjectInputStream(cs.getInputStream)
    //    写 输出流
    val oos: ObjectOutputStream = new ObjectOutputStream(cs.getOutputStream)
    var flg = true
    while (true) {
      val msg: AnyRef = ois.readObject()
      val rmsg = msg.asInstanceOf[RemoteMsg]
      //    模式匹配
      var rslt = rmsg match {
        case RegisterMsg(account, password) => {
          handlerRegister(account, password)
        }
        case HeartBeatMsg(clientId, content) => {
          handlerHeartbeat(clientId, content)
        }
      }
      oos.writeObject(rslt)
      oos.flush()
    }

    oos.close()
    ois.close()
    cs.close()
    ss.close()
  }
}

object RpcServerLocal {

  def main(args: Array[String]): Unit = {
    val server = new RpcServerLocal
    server.bind(8080)
  }

}

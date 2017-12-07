package com.rpc.client

import java.net.Socket
import java.net.ServerSocket
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import com.rpc.server.RegisterMsg
import com.rpc.server.ResultMsg

/**
 * 取自xn学堂
 */
class RpcClientLocal {

  def connect(host: String, port: Int): Socket = {
    new Socket(host, port)
  }

}

object RpcClientLocal {

  def main(args: Array[String]): Unit = {
    val client = new RpcClientLocal
    val socket: Socket = client.connect("localhost", 8080)
    //    获取输出流
    val oos: ObjectOutputStream = new ObjectOutputStream(socket.getOutputStream)
    val ois: ObjectInputStream = new ObjectInputStream(socket.getInputStream)

    oos.writeObject(RegisterMsg("zhaotf", "123456"))
    oos.flush()

    // 获取响应
    val rslt: AnyRef = ois.readObject()
    rslt match {
      case ResultMsg(result) => {
        println("客户端获取响应successs", result)
      }
    }

    oos.close()
    ois.close()
    socket.close()

  }

}

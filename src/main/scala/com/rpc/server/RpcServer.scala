package com.rpc.server

import java.net.ServerSocket
import java.net.Socket
import java.io.DataOutputStream
import java.io.ObjectInputStream
import java.io.DataInputStream

/**
 * 模拟rpc通信的服务端程序
 * @see (@link http://blog.csdn.net/zhou_shaowei/article/details/70879526)
 */
object RpcServer {

  def main(args: Array[String]): Unit = {
    //    建立socket通信
    val lis: ServerSocket = new ServerSocket(8080)
    println("服务端正在服务中.......")
    while (true) {
      //      建立socket通信
      val socket: Socket = lis.accept()
      //      创建输入输出流
      val out = new DataOutputStream(socket.getOutputStream)
      val in = new ObjectInputStream(new DataInputStream(socket.getInputStream()))

      val read: String = in.readUTF()
      println("客户端发送内容：", read)

      val ma: Array[String] = read.split(" ")
      val rslt = ma match {
        case Array("SubmitTask", id, name) => {
          s"$id,$name"
        }
        case Array("HeartBeat", time) => {
          s"$time"
        }
        case Array("CheckTimeOutTask") => {
          "check"
        }
        case _ => {
          "error"
        }
      }

      //      将服务端的结果写回客户端
      out.writeUTF(rslt.toString())
      out.flush()
      out.close()
      in.close()
      socket.close()
    }

  }

}
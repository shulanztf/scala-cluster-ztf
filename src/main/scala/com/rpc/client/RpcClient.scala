package com.rpc.client

import java.net.InetAddress
import java.net.Socket
import java.io.ObjectOutputStream
import java.io.DataInputStream

/**
 * 模拟rpc通信的客户端
 * @see (@link http://blog.csdn.net/zhou_shaowei/article/details/70879526)
 */
class RpcClient {

}

object RpcClient {
  def main(args: Array[String]): Unit = {
    //    创建socket通信
    val ia: InetAddress = InetAddress.getByName("localhost")
    val socket: Socket = new Socket(ia, 8080)
    //    建立输入输出流
    val out: ObjectOutputStream = new ObjectOutputStream(socket.getOutputStream)
    val in: DataInputStream = new DataInputStream(socket.getInputStream)
    //    数据
    val a = "HeartBeat 12ztf"
    println("test data", a)
    //    向服务端发送获取到的参数
    out.writeUTF(a)
    out.flush()

    //    接受服务端的返回结果
    val rslt: String = in.readUTF()
    println("服务端返回结果：", rslt)
    in.close()
    out.close()
    socket.close()
  }
}

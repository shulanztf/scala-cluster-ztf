package com.sutdy.spark

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source
import scala.util.Random

/**
  * 流数据模拟器
  * @see http://www.cnblogs.com/shishanyuan/p/4747749.html
  */
class StreamingSimulation {

}

object StreamingSimulation {
  //定义随机获取整数的方法
  def index(length: Int) = {
    val rdm = new Random()
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {
    // 调用该模拟器需要三个参数，分为为文件路径、端口号和间隔时间（单位：毫秒）
    if (args.length != 3) {
      System.err.println("Usage: <filename> <port> <millisecond>")
      System.exit(1)
    }

    // 获取指定文件总的行数
    val filename = args(0)
    val lines = Source.fromFile(filename).getLines().toList
    val filerow = lines.length

    // 指定监听某端口，当外部程序请求时建立连接
    val listener = new ServerSocket(args(1).toInt)
    while (true) {
      val socket = listener.accept()
      new Thread() {
        override def run(): Unit = {
          println("from:", socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream, true)
          while (true) {
            Thread.sleep(args(2).toLong)
            // 当该端口接受请求时，随机获取某行数据发送给对方
            val context = lines(index(filerow))
            println(context)
            out.write(context + "\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
    }

  }

}


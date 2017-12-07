package com.rpc.server

import java.io.BufferedReader
import java.net.Socket
import java.io.PrintStream
import collection.mutable
import java.net.ServerSocket
import java.io.DataInputStream
import java.io.File
import java.io.FileOutputStream

/**
 * 基于Socket的简单聊天服务器，实现代理功能，加入客户端自定义名字。利用Scala中的Actor实现并发。
 * @see (@link https://www.cnblogs.com/dingdaheng/p/5557494.html)
 */
class SimpleNet {

}

object SimpleNet extends App {
  def port = 8899
  override def main(args: Array[String]) {
    val roots = args.toList match {
      case Nil => List(new File(System.getProperty("user.dir")))
      case _ => args.map(new File(_)).toList
    }

    printf("接收dst root is %s\n", roots.mkString(","))
    val ss = new ServerSocket(port)
    def accept: Unit = {
      val s = ss.accept()
      printf("%s in \n", s.getRemoteSocketAddress);
      async {
        val dis = new DataInputStream(s.getInputStream)
        val fn = dis.readUTF
        val size = dis.readLong
        printf("loading %s %d\n", fn, size);
        if (size > 0) {
          roots.foreach(new File(_, fn).getParentFile.mkdirs())
          val oses = roots.map(new File(_, fn)).map(new FileOutputStream(_))
          val buf = new Array[Byte](1024)
          var done = false
          while (!done) {
            val r = dis.read(buf)
            if (r <= 0) done = true
            else oses.foreach(_.write(buf, 0, r))
          }
          oses.foreach(_.close)
        }
      }
      accept
    }
    accept
  }

  def async(body: => Unit) = {
    val t = new Thread(new Runnable {
      override def run(): Unit = {
        body
      }
    })
    t.start()
  }

}

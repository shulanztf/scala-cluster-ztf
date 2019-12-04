package com.rpc.client

import java.net.Socket
import java.io.FileInputStream
import java.io.DataOutputStream
import java.io.File

/**
 * @see (@link https://www.cnblogs.com/dingdaheng/p/5557494.html)
 */
class ChatClient {

}

object ChatClient extends App {
  def port = 8899

  def send_file(ip: String, file: File, top: String): Unit = {
    val fis = new FileInputStream(file)
    val s = new Socket(ip, port)
    val dos = new DataOutputStream(s.getOutputStream)
    val remotename = top + File.separator + file.getName
    printf("发送sending %d bytes %s to %s\n", file.length(), file.getAbsolutePath, remotename)
    dos.writeUTF(remotename)
    dos.writeLong(file.length())
    val buf = new Array[Byte](1024)
    var done = false
    while (!done) {
      val r = fis.read(buf)
      if (r <= 0) done = true
      else dos.write(buf, 0, r)
    }
    dos.close()
    fis.close()
    s.close()

  }

  def send(ip: String, file: File, top: String): Unit = {
    if (file.isDirectory) file.listFiles().foreach(send(ip, _, "" + File.separator + file.getName))
    else send_file(ip, file, top)

  }

  override def main(args: Array[String]) {
//    if (args.length < 1) usage()
//    else args.drop(1).map(new File(_)).foreach(send(args(0), _, ""))
    send("localhost",new File("D:/data/ttt.csv"),"abc")
  }

  def usage(): Unit = {
    println("usage: jpsh.Client <ip> [file]...")
    System.exit(-1)
  }
}

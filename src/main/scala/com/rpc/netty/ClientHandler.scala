package com.rpc.netty

import io.netty.channel.ChannelInboundHandler
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.concurrent.atomic.AtomicLong

/**
 * XN学堂，scala/netty
 */
class ClientHandler extends ChannelInboundHandlerAdapter {
  val id = (new AtomicLong).addAndGet(System.currentTimeMillis()).toString()

  /**
   * 跟服务器建立连接后被调用
   */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("连接已建立")
    val cont = "content test……"
//        ctx.writeAndFlush(Unpooled.copiedBuffer(cont.getBytes("UTF-8")))
    ctx.writeAndFlush(RegisterMsg(id, "he!请求注册"))
  }

  /**
   * 接收 消息
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    //    val byteBuf = msg.asInstanceOf[ByteBuf]
    //    val bytes = new Array[Byte](byteBuf.readableBytes())
    //    byteBuf.readBytes(bytes)
    //    val message = new String(bytes, "UTF-8")
    //    println(message)
    //    msg match {
    //      case ResposeMsg(cont) => {
    //        println("收到服务端响应", cont)
    //      }
    //    }
    partFun(msg)
  }

  /**
   * 消息统一发送
   */
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    println("消息统一发送 执行")
    ctx.flush()
  }

  /**
   * 异常处理
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    println("异常发生", cause)
    ctx.close()
  }

  /**
   * 偏函数，处理消息用
   */
  val partFun: PartialFunction[scala.Any, NettyMessage] = {
    case ResposeMsg(cont) => {
      //      ResposeMsg("服务端响应，注册成功")
      println("收到服务端响应", cont)
      ConfrimMsg()
    }
    case _ => {
      println("消息类型错误 ")
      ConfrimMsg()
    }
  }

}

object ClientHandler {

  def main(args: Array[String]): Unit = {

  }

}

package com.rpc.netty

import io.netty.channel.ChannelInboundHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.util.regex.UnicodeProp
import java.nio.charset.Charset
import io.netty.channel.ChannelInboundHandlerAdapter

/**
 * XN学堂，scala/netty
 */
class ServerHandler extends ChannelInboundHandlerAdapter {
  /**
   * 客户端建立连接后用
   */
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("发现客户端建立连接")
  }

  /**
   * 接收 客户发送的消息，业务逻辑代码在引,TODO
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    println("接收到客户端的消息", msg)
    //    val byteBuf = msg.asInstanceOf[ByteBuf]
    //    println("aa", byteBuf)
    //    val bytes = new Array[Byte](byteBuf.readableBytes())
    //    println("bb", bytes)
    //    byteBuf.readBytes(bytes)
    //    println("cc", bytes)
    //    val message = new String(bytes, "UTF-8")
    //    println("接收到客户端的消息", message, msg)
    //    msg match {
    //      case RegisterMsg(workerId, cont) => {
    //        println("注册消息", workerId, cont)
    //        ctx.write(ResposeMsg("服务端响应，注册成功"))
    //      }
    //    }
    //    if (msg.isInstanceOf[NettyMessage]) {
    //    	partFun(msg)
    //    }

    //    val back = "good boy!服务端中文a"
    //    ctx.write(Unpooled.copiedBuffer(back.getBytes("UTF-8")))
    ctx.write(partFun(msg))
  }

  /**
   * 将消息队列中的数据，写入到SocketChannel，并发送给对方
   */
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    println("消息统一发送 执行")
    ctx.flush()
  }

  /**
   * 异步时关闭ChannelHandlerContext
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    println("异常发生")
    ctx.close()
  }

  /**
   * 偏函数，处理消息用
   */
  val partFun: PartialFunction[scala.Any, NettyMessage] = {
    case RegisterMsg(workerId, cont) => {
      println("接收到客户端注册消息", workerId, cont)
      ResposeMsg("服务端响应，注册成功")
    }
    case HeartBeatMsg(workerId: String, content: String) => {
      println("接收到客户端心跳消息", workerId, content)
      ResposeMsg("心跳更新成功")
    }
    case _ => {
      println("消息类型错误 ")
      ResposeMsg("消息类型错误 ")
    }
  }

}

object ServerHandler {

}

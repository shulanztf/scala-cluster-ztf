package com.rpc.client

import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled

/**
 * @see (@link https://www.cnblogs.com/itboys/p/6077640.html)
 */
class ClientHandler extends ChannelInboundHandlerAdapter {
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("channelActive")
    val content = "hello server不在发送字符串了，封装一个字符串"
    ctx.writeAndFlush(Unpooled.copiedBuffer(content.getBytes("UTF-8")))
    //发送case class 不在发送字符串了，封装一个字符串
    //    ctx.writeAndFlush(RegisterMsg("hello server"))
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    println("channelRead")
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val message = new String(bytes, "UTF-8")
    println(message)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    println("channeReadComplete")
    ctx.flush()
  }
  //发送异常时关闭
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    println("exceptionCaught")
    ctx.close()
  }

}
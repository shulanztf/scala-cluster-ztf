package com.rpc.server

import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelHandlerContext
import io.netty.buffer.Unpooled
import io.netty.buffer.ByteBuf

/**
 * @see (@link https://www.cnblogs.com/itboys/p/6077640.html)
 */
class ServerHandler extends ChannelInboundHandlerAdapter {

  /**
   * 有客户端建立连接后调用
   */
  override def channelActive(etx: ChannelHandlerContext): Unit = {
    println("有客户端建立连接后调用")
  }

  /**
   * 接受客户端发送来的消息
   */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    println("channelRead invoked")
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val message = new String(bytes, "UTF-8")
    println(message)
    val back = "good boy!"
    val resp = Unpooled.copiedBuffer(back.getBytes("UTF-8"))
    println(msg)
    ctx.write(resp)
  }

  /**
   * 将消息对列中的数据写入到SocketChanne并发送给对方
   */
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    println("channekReadComplete invoked")
    ctx.flush()
  }

}
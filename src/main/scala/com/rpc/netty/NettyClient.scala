package com.rpc.netty

import io.netty.channel.nio.NioEventLoop
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.bootstrap.Bootstrap
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.serialization.ObjectEncoder
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.codec.serialization.ClassResolver
import io.netty.handler.codec.serialization.ClassResolvers

/**
 * XN学堂，scala/netty
 */
class NettyClient {

  def connect(host: String, port: Int): Unit = {
    //    创建客户端NIO线程组
    val eventGroup = new NioEventLoopGroup
    //    创建客户端辅助启动类
    val bootstrap = new Bootstrap
    try {
      //    将NIO线程组传入到bootstrap
      bootstrap.group(eventGroup)
        //    创建Niosocketchannel
        .channel(classOf[NioSocketChannel])
        //    绑定IO事件片类
        .handler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(
              new ObjectEncoder,
              new ObjectDecoder(ClassResolvers.cacheDisabled(getClass.getClassLoader)),
              new ClientHandler() //业务埋点
            )
          }
        })
      //    发起异步操作
      val channelFuture = bootstrap.connect(host, port).sync()
      //    等待服务关闭
      channelFuture.channel().closeFuture().sync()
    } finally {
      //      关闭资源
      eventGroup.shutdownGracefully()
    }
  }

}

object NettyClient {

  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val client = new NettyClient()
    client.connect(host, port)
  }

}

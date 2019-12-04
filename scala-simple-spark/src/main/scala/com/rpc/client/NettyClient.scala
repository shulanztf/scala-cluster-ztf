package com.rpc.client

import io.netty.channel.nio.NioEventLoopGroup
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

/**
 * @see (@link https://www.cnblogs.com/itboys/p/6077640.html)
 */
class NettyClient {
  def connect(host: String, port: Int): Unit = {
    //创建客户端NIO线程组
    val eventGroup = new NioEventLoopGroup
    //创建客户端辅助启动类
    val bootstrap = new Bootstrap
    try {
      //将NIO线程组传入到Bootstrap
      bootstrap.group(eventGroup)
        //创建NioSocketChannel
        .channel(classOf[NioSocketChannel])
        //绑定I/O事件处理类
        .handler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(
              //            new ObjectEncoder,
              //            new ObjectDecoder(ClassResolvers.cacheDisabled(getClass.getClassLoader)),
              new ClientHandler)
          }
        })
      //发起异步连接操作
      val channelFuture = bootstrap.connect(host, port).sync()
      //等待服务关闭
      channelFuture.channel().closeFuture().sync()
    } finally {
      //优雅的退出，释放线程池资源
      eventGroup.shutdownGracefully()
    }
  }
}

object NettyClient {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = 8080
    val client = new NettyClient
    client.connect(host, port)
  }
}

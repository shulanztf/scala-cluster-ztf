package com.rpc.netty

import io.netty.channel.nio.NioEventLoopGroup
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.serialization.ObjectEncoder
import io.netty.handler.codec.serialization.ObjectDecoder
import io.netty.handler.codec.serialization.ClassResolvers

/**
 * XN学堂，scala/netty
 */
class NettyServer {

  def bind(host: String, port: Int) {
    //配置服务器线程组
    //    服务器接收 客户端的连接
    val bossGroup = new NioEventLoopGroup()
    //    用户进行socketChannel的网络读写
    val workerGroup = new NioEventLoopGroup
    //    netty用户启动NIO服务端的辅助启动类，降低服务端的开发复杂度
    val bootstrap = new ServerBootstrap
    try {
      bootstrap.group(bossGroup, workerGroup)
        //    创建NIO channel
        .channel(classOf[NioServerSocketChannel])
        //    绑定IO事件处理
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(
              new ObjectEncoder, //转码处理
              new ObjectDecoder(ClassResolvers.cacheDisabled(getClass().getClassLoader)),
              new ServerHandler() //业务埋点 TODO
            )
          }
        })
      //      绑定端口，调用 sync方法等待绑定操作完成
      val channelFuture = bootstrap.bind(host, port).sync()
      //      等待服务关闭
      channelFuture.channel().closeFuture().sync()
    } finally {
      //      优雅的退出，释放线程池资源
      bossGroup.shutdownGracefully()
      workerGroup.shutdownGracefully()
    }

  }

}

object NettyServer {

  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val server = new NettyServer()
    server.bind(host, port)
  }

}

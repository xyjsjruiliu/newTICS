package com.xy.lr.scala.socket

import java.net.{Socket, ServerSocket}

/**
 * Created by xylr on 15-3-3.
 */

object Server{
  def main(args : Array[String]): Unit ={
    val server : SocketServer = new SocketServer(9999)
    server.beginListen("10000")
  }
}

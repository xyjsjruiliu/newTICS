package com.xy.lr.java.zeromq

import org.apache.spark.rdd.RDD
import org.zeromq.ZMQ

/**
 * Created by xylr on 15-4-7.
 */
class SparkSQLServerThread extends Thread{
  var rdd : SparkSQLServer = _
  def this(rdd : SparkSQLServer){
    this()
    this.rdd = rdd
  }
  override def run(): Unit ={
    val context: ZMQ.Context = ZMQ.context(1)
    val socket: ZMQ.Socket = context.socket(ZMQ.REP)

    @transient var i = 1
    socket.bind("tcp://*:5555")
    while (true) {
      var request: Array[Byte] = null
      request = socket.recv(0)
      System.out.println("Received request: [" + new String(request, 0, request.length - 1) + "]")
      try {
        Thread.sleep(1000)
      }
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
      val s = rdd.getRDD.collect()(0)

      val replyString: String = s + " "
      val reply: Array[Byte] = replyString.getBytes
      reply(reply.length - 1) = 0.toByte
      socket.send(reply, 0)
      i = i % 10 + 1
    }
  }
}

package com.xy.lr.java.zeromq

import org.zeromq.ZMQ

/**
 * Created by xylr on 15-4-7.
 */
class SparkSQLClient {

}
object SparkSQLClient{
  def main(args : Array[String]): Unit ={
    //  Prepare our context and socket
    val context: ZMQ.Context = ZMQ.context(1)
    val socket: ZMQ.Socket = context.socket(ZMQ.REQ)

    System.out.println("Connecting to hello world server...")
    socket.connect("tcp://localhost:5555")
    //  Do 10 requests, waiting each time for a response
      var request_nbr: Int = 0
      while (request_nbr != 10) {
        {
          val requestString: String = "Hello" + " "
          val request: Array[Byte] = requestString.getBytes
          request(request.length - 1) = 0.toByte
          System.out.println("Sending request " + request_nbr + "...")
          socket.send(request, 0)
          val reply: Array[Byte] = socket.recv(0)
          System.out.println("Received reply " + request_nbr + ": [" +
            new String(reply, 0, reply.length - 1) + "]")
          request_nbr = request_nbr + 1
        }

    }
  }
}

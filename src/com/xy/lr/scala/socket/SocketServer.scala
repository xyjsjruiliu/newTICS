package com.xy.lr.scala.socket

import java.io.{IOException, PrintWriter}
import java.net.{Socket, ServerSocket}
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by xylr on 15-3-5.
 */
class SocketServer {
  private[socket] var sever: ServerSocket = null

  def this(port: Int) {
    this()
    try {
      sever = new ServerSocket(port)
    }
    catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
  }

  def getCurrentTime: String = {
    val date: Date = new Date
    val df: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    df.format(date)
  }

  def index = {
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(10000)
  }

  def beginListen(carNUmber: String) {
    val file = Source.fromFile("/home/xylr/project/112")
    val line = file.getLines()
    val array = ArrayBuffer[String]()
    for(j <- line){
      array += j
    }
    while (true) {
      var t: Thread = null
      try {
        val socket: Socket = sever.accept
        System.out.println("Got client connect from : " + socket.getInetAddress)
        System.out.println("Start Send Meg")
        var CarNumber: Long = carNUmber.toLong
        t = new Thread {
          override def run() {
            try {
              var i: Int = 0
              val out: PrintWriter = new PrintWriter(socket.getOutputStream)

              while (!socket.isClosed) {
                try {
                  Thread.sleep(1000)
                }
                catch {
                  case e: InterruptedException => {
                    e.printStackTrace()
                  }
                }
                if(i % array.length == 0){
                  i = 1
                  CarNumber = CarNumber + 1
                }
                val time: String = getCurrentTime
                out.println("start" + "," + CarNumber + "," + time + "," + array(i) + ",N" )
                println("start" + "," + CarNumber + "," + time + "," + array(i) + ",N")
                i += 1
                out.flush()
//                if (socket.isClosed) {
//                  return
//                }
              }
              println("disconnect from : " + socket.getInetAddress)
              socket.close()
            }
            catch {
              case e: IOException => {
                e.printStackTrace()
              }
            }
          }
        }
        t.start()
      }
      catch {
        case e: IOException => {
          e.printStackTrace()
          t.stop()
        }
      }
    }
  }
}

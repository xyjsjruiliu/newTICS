package com.xy.lr.tics.dezhouserver

import java.io.{FileNotFoundException, IOException, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.text.SimpleDateFormat
import java.util.Date

import com.xy.lr.tics.properties.TICSInfo

import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

/**
 * Created by xylr on 15-3-5.
 */
class DeZhouSocketServer {
  private var sever: ServerSocket = null
  private var ticsInfo : TICSInfo = _
  private var time : Long = _

  def this(path: String, time : Long) {
    this()
    try {
      ticsInfo = new TICSInfo(path)
      sever = new ServerSocket(ticsInfo.getDeZhouServerPort.toInt)
      this.time = time
    }
    catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
  }

  def getCurrentTime: String = {
    val date: Date = new Date
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(date)
  }

  //not used
  def index = {
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(10000)
  }

  def beginListen(carNumber: String) {
    var file : BufferedSource = null
    try{
      file = Source.fromFile("/home/xylr/project/112")
    }catch{
      case e: FileNotFoundException => {
        System.err.println("车辆路径文件 file not found!\n" + "默认路径是 /home/xylr/project/112")
      }
      case _ => {
        System.err.println("load 车辆路径文件 error")
      }
    }
    val line = file.getLines()
    val array = ArrayBuffer[String]()
    for(j <- line){
      array += j
    }
    println("start [ DeZhouServer ] at : " + getCurrentTime)
    while (true) {
      var t: Thread = null
      try {
        val socket: Socket = sever.accept
        System.out.println("Got client connect from : " + socket.getInetAddress)
        System.out.println("Start Send Meg")
        var CarNumber: Long = carNumber.toLong
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
                  case e: InterruptedException =>
                    e.printStackTrace()
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

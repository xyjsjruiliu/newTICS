package com.xy.lr.tics.spark.sql

import java.text.SimpleDateFormat
import java.util.Date
import com.xy.lr.tics.properties.TICSInfo
import com.xy.lr.tics.spark.SparkEngine
import org.zeromq.ZMQ

/**
 * Created by xylr on 15-4-7.
 */
class SparkSQLServer extends Thread{
  private var sparkEngine : SparkEngine = _
  private var ticsInfo : TICSInfo = _
  private val sparkSQLServerContext : ZMQ.Context = ZMQ.context(1)
  private val sparkSQLServerSocket : ZMQ.Socket = sparkSQLServerContext.socket(ZMQ.REP)

  def this(sparkEngine : SparkEngine, infoPath : String){
    this()
    //SparkEngine
    this.sparkEngine = sparkEngine
    //导入配置文件
    ticsInfo = new TICSInfo(infoPath)
    //SparkSQLServer 服务器绑定端口
    sparkSQLServerSocket.bind("tcp://*:" + ticsInfo.getSparkSQLServerPort)
  }
  def getCurrentTime : String = {
    val date = new Date()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val times = df.format(date)
    times
  }
  def makeReply(req : String) : Array[Byte] = {
    val requestString: String = req + " "
    val request: Array[Byte] = requestString.getBytes
    request(request.length - 1) = 0.toByte
    request
  }
  def getRequest(req : Array[Byte]) : String = {
    new String(req, 0, req.length - 1)
  }
  def select(request : String) : String = {
    if(request.startsWith("CG")){
      sparkEngine.getCarGraph(request.substring(2,request.length))
    }
    else if(request.startsWith("sql")){
      sparkEngine.sql(request.substring(3, request.length))
    }
    else{
      ""
    }
  }
  override def run(): Unit ={
//    val context:  ZMQ.Context = ZMQ.context(1)
//    val socket: ZMQ.Socket = zmqContext.socket(ZMQ.REP)
    println("start [ SparkSQLServer ] at " + getCurrentTime)
//    @transient var i = 1

    while (true) {
      var request: Array[Byte] = null
      //接受查询
      request = sparkSQLServerSocket.recv(0)
      val requestString = getRequest(request)

      System.out.println("Received request: [" + getRequest(request) + "]")


      try {
        Thread.sleep(1000)
      }
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
//      val rdd = sparkEngine.getRDD.collect()(0)
//      val name = sparkEngine.getPeoplesName
      //查询
      val replyString: String = select(requestString) + " "

      val reply: Array[Byte] = makeReply(replyString)

      //返回查询结果
      sparkSQLServerSocket.send(reply, 0)
//      i = i % 10 + 1
    }
  }
}
/*
object SparkSQLServer{

  def main(args : Array[String]): Unit ={



  }
}
*/
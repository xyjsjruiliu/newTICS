package com.xy.lr.tics.spark.sql

import java.text.SimpleDateFormat
import java.util.Date

import com.xy.lr.tics.properties.TICSInfo
import org.zeromq.ZMQ

/**
 * Created by xylr on 15-4-7.
 */
class SparkSQLClient {
  private var ticsInfo : TICSInfo = _
  private val sparkSQLClientcontext : ZMQ.Context = ZMQ.context(1)
  private val sparkSQLClientSocket : ZMQ.Socket = sparkSQLClientcontext.socket(ZMQ.REQ)

  def this(path : String) {
    this()
    //导入配置文件
    ticsInfo = new TICSInfo(path)
    //连接SparkSQLServer
    sparkSQLClientSocket.connect("tcp://" + ticsInfo.getSparkSQLServerUrl +
      ":" + ticsInfo.getSparkSQLServerPort)
  }

  def getCurrentTime : String = {
    val date = new Date()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val times = df.format(date)
    times
  }
  def makeRequest(req : String) : Array[Byte] = {
    val requestString: String = req + " "
    val request: Array[Byte] = requestString.getBytes
    request(request.length - 1) = 0.toByte
    request
  }
  def getReply(rep : Array[Byte]) : String = {
    new String(rep, 0, rep.length - 1)
  }
  def test(req : String): String ={
    System.out.println("Connecting to [ SparkSQLServer ] at " + getCurrentTime)

    val request: Array[Byte] = makeRequest(req)

    System.out.println("Sending request : " + req)
    //发送查询请求
    sparkSQLClientSocket.send(request, 0)

    //接受查询结果
    val reply : Array[Byte] = sparkSQLClientSocket.recv(0)
    System.out.println("Received reply " + ": [" + getReply(reply) + "]")

    val result = getReply(reply)
    //返回结果
    result
  }
  /*
  def getGraphByCarNumber(carNumber : String) : String = {
    ""
  }
  */

  def sql(query: String): String = {
    test("sql" + query)
  }

  def getCarGraph(carNumber: String): String = {
    test("CG" + carNumber)
  }
}
/*
object SparkSQLClient{
  def main(args : Array[String]): Unit ={

  }
}
*/
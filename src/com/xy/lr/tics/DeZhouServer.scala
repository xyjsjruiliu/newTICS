package com.xy.lr.tics

import com.xy.lr.tics.dezhouserver.DeZhouSocketServer

/**
 * Created by xylr on 15-3-3.
 */

object DeZhouServer{
  def main(args : Array[String]): Unit ={
    //模拟卡口服务器，监听9999端口号，发送间隔时间1000ms
    val dezhouserver : DeZhouSocketServer = new DeZhouSocketServer(
      "TICSInfo.properties", 1000)
    //初始车牌号
    dezhouserver.start()
  }
}

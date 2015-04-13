package com.xy.lr.tics.spark

import java.io.{IOException, InputStreamReader, BufferedReader}
import java.net.{UnknownHostException, Socket}

/**
 * Created by xylr on 15-4-11.
 */
class DeZhouClient {
  private[spark] var client: Socket = null

  def this(site: String, port: Int) {
    this()
    try {
      client = new Socket(site, port)
    }
    catch {
      case e: UnknownHostException =>
        e.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    }
  }

  def getMsg: String = {
    try {
      val in: BufferedReader = new BufferedReader(
        new InputStreamReader(client.getInputStream))
      in.readLine
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
    ""
  }

  def closeSocket() : Unit = {
    try {
      client.close()
    }
    catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
}

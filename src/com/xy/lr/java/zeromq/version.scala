package com.xy.lr.java.zeromq

import org.zeromq.ZMQ

/**
 * Created by xylr on 15-4-7.
 */
class version {

}
object version{
  def main(args : Array[String]): Unit ={
    printf("Version string: %s, Version int: %d\n", ZMQ.getVersionString, ZMQ.getFullVersion)
  }
}

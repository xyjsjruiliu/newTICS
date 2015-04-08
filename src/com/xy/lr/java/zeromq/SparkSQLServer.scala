package com.xy.lr.java.zeromq

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.zeromq.ZMQ

/**
 * Created by xylr on 15-4-7.
 */
class SparkSQLServer extends Thread{
  private var rdd : RDD[Int] = _
  def getRDD = {
    rdd
  }
  override def run(): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("ClientApp")
    val sc = new SparkContext(conf)

    @transient var i = 1
    rdd = sc.parallelize(List(i))

    while(true){
      Thread.sleep(1000)
      rdd = sc.parallelize(List(i))
      i = i + 1
    }
  }
}
object SparkSQLServer{

  def main(args : Array[String]): Unit ={
    val sparkSQLServer = new SparkSQLServer()
    sparkSQLServer.start()

    Thread.sleep(10000)
    val t = new SparkSQLServerThread(sparkSQLServer)
    t.start()

  }
}

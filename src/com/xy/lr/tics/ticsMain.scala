package com.xy.lr.tics

import com.xy.lr.tics.properties.TICSInfo
import com.xy.lr.tics.spark.SparkEngine
import com.xy.lr.tics.spark.sql.SparkSQLServer

/**
 * Created by xylr on 15-4-11.
 */
object ticsMain {
  def main(args : Array[String]): Unit ={
    /*
    if(args.length != 1){
      println("Usage : spark-submit " +
        "--master spark://localhost:7077 " +
        "--class com.xy.lr.tics.ticsMain TICSServer.jar localhost 9999 10 HBaseInfo.properties StreamingLogs")
      System.exit(0)
    }*/
    //Engine start
    val sparkEngine = new SparkEngine("local[2]", "DefaultName", "TICSInfo.properties")
    sparkEngine.start()

    Thread.sleep(10000)
    //start SparkSQLServer
    val t = new SparkSQLServer(sparkEngine, "TICSInfo.properties")
    t.start()
  }
}

package com.xy.lr.tics

import com.xy.lr.tics.spark.sql.{SparkSQLClientJava, SparkSQLClient}

/**
 * Created by xylr on 15-4-11.
 */
object sqlCllient {
  def main(args : Array[String]): Unit ={
    val sqlClient = new SparkSQLClientJava("TICSInfo.properties")
    println(sqlClient.getCarGraph("10001"))
    println(sqlClient.sqlBlackList())
//    println(sqlClient.sql("select graph from car"))
    println(sqlClient.sqlGrapgByBlackList("10004"))
  }
}

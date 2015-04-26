package com.xy.lr.tics.json

import com.xy.lr.java.json.{JSONArray, JSONObject}

/**
 * Created by xylr on 15-4-26.
 * 套牌车Json
 */
class fakeCarJson {
  //状态码
  private var status : String = _
  //套牌车轨迹
  private var fakeCarTrace : String = _

  //构造函数
  def this(status : String, fakeCarTrace : String){
    this()
    this.status = status
    this.fakeCarTrace = fakeCarTrace
  }

  //update
  def setStatus(newStatus : String): Unit ={
    this.status = newStatus
  }

  //update
  def setFakeCarTrace(newFakeCarTrace : String): Unit ={
    this.fakeCarTrace = newFakeCarTrace
  }

  //查询疑似套牌车报警日志
  def getFakeCarTraceJson : String = {
    val fakeCarTraceJson = new JSONObject()
    fakeCarTraceJson.put("status", status)

    val fakeCarTraceJsonArray = new JSONArray()

    //使用;作为每个套牌车分割符
    fakeCarTrace.split(";").map( x => {
      if(x.split(",").length != 3){
        println("error")
      }
      else{
        val carNo = x.split(",")(0)
        val time  = x.split(",")(1)
        val placeNo = x.split(",")(2)
        val fakeCar = new JSONObject()
        fakeCar.put("carNo", carNo)
        fakeCar.put("time", time)
        fakeCar.put("placeNo", placeNo)

        fakeCarTraceJsonArray.put(fakeCar)
      }
    })

    fakeCarTraceJson.put("alarmData", fakeCarTraceJsonArray)

    fakeCarTraceJson.toString
  }
}
/*
object fakeCarJson{
  def main(args : Array[String]): Unit ={
    val fk = new fakeCarJson(StatusOfJson.SUCCESS, "1,2,3;3,4,5;2,3")
    println(fk.getFakeCarTraceJson)
  }
}
*/
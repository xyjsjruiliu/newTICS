package com.xy.lr.tics.json

import com.xy.lr.java.json.{JSONArray, JSONObject}


/**
 * Created by xylr on 15-4-26.
 * 车辆轨迹Json
 */
class carTraceJson {
  //状态码,车牌号
  private var carInfo : addAndDeleteBLJson = _
  //车辆轨迹
  private var carTrace : String = _

  //构造函数
  def this(status : String, carNo : String, carTrace : String){
    this()
    this.carInfo = new addAndDeleteBLJson(status, carNo)
    this.carTrace = carTrace
  }

  //更新车辆信息
  def setCarInfo(status : String, carNo : String): Unit ={
    this.carInfo = new addAndDeleteBLJson(status, carNo)
  }

  //更新车辆轨迹
  def setCarTrace(carTrace : String): Unit ={
    this.carTrace = carTrace
  }

  def getCarTraceJsonArray(carTrace : String, i : String) : JSONObject = {
    val carJsonArrayTrace = new JSONObject()
    carJsonArrayTrace.put("traceNo", i)

    val carJsonArrayTraceArray = new JSONArray()

    carTrace.split(",").map( x => {
      val time = x.split(":")(0)
      val placeNo = x.split(":")(1)
      val temp = new JSONObject()
      temp.put("time", time)
      temp.put("placeNo", placeNo)
      carJsonArrayTraceArray.put(temp)
    })

    carJsonArrayTrace.put("trace", carJsonArrayTraceArray)
  }

  //得到车辆Json
  def getCarTraceJson : String = {
    val carJsonObject = new JSONObject()
    carJsonObject.put("status", carInfo.getStatus)
    carJsonObject.put("carNo", carInfo.getCarNo)

    val carJsonArray = new JSONArray()

    carJsonArray.put(getCarTraceJsonArray(carTrace, "1"))

    carJsonObject.put("traceArray", carJsonArray)

    carJsonObject.toString
  }
}
/*
object carTraceJson{
  def main(args : Array[String]): Unit ={
    val a = new carTraceJson(StatusOfJson.SUCCESS, "1000", "1:2,2:3")
    println(a.getCarTraceJson)
  }
}
*/
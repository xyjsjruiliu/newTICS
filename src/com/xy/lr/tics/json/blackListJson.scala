package com.xy.lr.tics.json

import com.xy.lr.java.json.{JSONObject, JSONArray}

/**
 * Created by xylr on 15-4-26.
 * 查询嫌疑车辆黑名单 json
 */
class blackListJson {
  //状态码
  private var status : String = _
  //黑名单列表
  private var blacklist : String = _

  def this(status : String, blacklist : String){
    this()
    this.status = status
    this.blacklist = blacklist
  }

  //重新设置状态码
  def setStatus(newStatus : String): Unit ={
    this.status = newStatus
  }

  //重新设置黑名单列表
  def setBlackList(newBlackList : String): Unit ={
    this.blacklist = newBlackList
  }

  //得到json类型的blacklist
  def getBlackListJson : String = {
    val bl = new JSONObject()
    bl.put("status", status)

    val bll = new JSONArray()
    blacklist.split(":").map( x => {
      val temp = new JSONObject()
      temp.put("carNo", x)
      temp.put("time", "0")

      bll.put(temp)
    })

    bl.put("blacklist", bll)
    bl.toString
  }
}
/*
object blackListJson{
  def main(args : Array[String]): Unit ={
    val blj = new blackListJson(StatusOfJson.SUCCESS, "1:2:3")
    println(blj.getBlackListJson)
  }
}
*/
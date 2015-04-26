package com.xy.lr.tics.json

import com.xy.lr.java.json.JSONObject

/**
 * Created by xylr on 15-4-26.
 * 添加或者删除 黑名单 中的车辆信息
 */
class addAndDeleteBLJson {
  //状态码
  private var status : String = _
  //车牌号
  private var carNo : String = _

  def this(status : String, carNo : String){
    this()
    this.status = status
    this.carNo = carNo
  }

  def setStatus(newStatus : String): Unit ={
    this.status = newStatus
  }

  def setCarNo(newCarNo : String): Unit ={
    this.carNo = newCarNo
  }

  def getStatus : String = {
    status
  }

  def getCarNo : String = {
    carNo
  }

  //
  def getAddBLJson : String = {
    val add = new JSONObject()
    add.put("status", status)
    add.put("carNo", carNo)
    add.toString
  }

  //
  def getDeleteJson : String = {
    val delete = new JSONObject()
    delete.put("status", status)
    delete.put("carNo", carNo)
    delete.toString
  }
}

package com.xy.lr.tics.properties

/**
 * Created by xylr on 15-4-16.
 * 车辆黑名单
 */
class BlackList extends java.io.Serializable{
  private var carNumber : String = _
  private var carGraph : CarInfo = _

  def this(carNumber : String, carGraph : CarInfo){
    this()
    this.carNumber = carNumber
    this.carGraph = carGraph
  }
  def getCarNumber : String = {
    carNumber
  }
  def getCarGraph : CarInfo = {
    carGraph
  }
}

package com.xy.lr.tics.properties

/**
 * Created by xylr on 15-3-13.
 */
class OldMessage extends java.io.Serializable{
  private var carNumber : String = _
  private var oldMessageRoute : String = _

  def this(carNumber : String, oldMR : String){
    this()
    this.carNumber = carNumber
    this.oldMessageRoute = oldMR
  }
  def getCarNumber : String = {
    carNumber
  }
  def getOldMessageRoute : String = {
    oldMessageRoute
  }
}

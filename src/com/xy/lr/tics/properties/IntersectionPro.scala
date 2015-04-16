package com.xy.lr.tics.properties

/**
 * Created by xylr on 15-4-14.
 * 任意两个卡口的最短路径和长度
 */
class IntersectionPro extends java.io.Serializable{
  //源卡口ID
  private var sourceID : Long = _
  //目标卡口ID
  private var destID : Long = _
  //路径长度
  private var length : Long = _
  //最短路径
  private var route : String = _

  def this(sourceID : Long, destID : Long, length : Long,
            route : String){
    this()
    this.sourceID = sourceID
    this.destID = destID
    this.length = length
    this.route = route
  }

  def getSourceID : Long = {
    sourceID
  }
  def getDestID : Long = {
    destID
  }
  def getLength : Long = {
    length
  }
  def getRoute : String = {
    route
  }
}

package com.xy.lr.tics.properties

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by xylr on 15-3-9.
 */
class MapVertexInfo extends java.io.Serializable{
  //顶点编号
  private var VertexNumber : Long = _
  //纬度
  private var Latitude : Double = _
  //经度
  private var Longitude : Double = _
  def this(vn : Long, lat : Double, long : Double){
    this()
    this.VertexNumber = vn
    this.Latitude = lat
    this.Longitude = long
  }
  def getVertexNumber : Long = {
    VertexNumber
  }
  def getLatitude : Double = {
    Latitude
  }
  def getLongitude : Double = {
    Longitude
  }

}

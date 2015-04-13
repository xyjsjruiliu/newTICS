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
  def getMapVertexRDD : ArrayBuffer[MapVertexInfo] = {
    val mapVertexRDD = new ArrayBuffer[MapVertexInfo]()
    val mapVertexFile = Source.fromFile(
      "/home/xylr/software/Data/guangzhou/cpp/vertex.txt")
    val line = mapVertexFile.getLines()
    for(i <- line){
      if(i.split("\t").length != 2){
        println("input error!!!")
      }
      else{
        val VertexNumber = i.split("\t")(0).toLong
        val Latitude = i.split("\t")(1).split(" ")(0).toDouble
        val Longitude = i.split("\t")(1).split(" ")(1).toDouble
        mapVertexRDD += new MapVertexInfo(VertexNumber, Latitude, Longitude)
      }
    }
    mapVertexRDD
  }
}

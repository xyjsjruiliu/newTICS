package com.xy.lr.tics.properties

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by xylr on 15-4-14.
 * SparkEngine 启动时需要使用的基本信息
 */
class EngineProperty extends java.io.Serializable{
  //配置文件
  private var ticsInfo : TICSInfo = _
  private var mapVertexInfoArray : ArrayBuffer[MapVertexInfo] = _
  private var mapEdgeInfoArray : ArrayBuffer[MapEdgeInfo] = _
  private var intersectionProArray : ArrayBuffer[IntersectionPro] = _

  def this(path : String){
    this()
    ticsInfo = new TICSInfo(path)
  }
  //初始化基本信息
  def initEngineProperty(): Unit ={
    mapVertexInfoArray = getMapVertex
    mapEdgeInfoArray = getMapEdge
    intersectionProArray = getIntersection
  }
  //get 卡口
  def getIntersectionProArray : ArrayBuffer[IntersectionPro] = {
    intersectionProArray
  }
  //get 顶点
  def getMapVertexInfoArray : ArrayBuffer[MapVertexInfo] = {
    mapVertexInfoArray
  }
  //get 边
  def getMapEdgeInfoArray : ArrayBuffer[MapEdgeInfo] ={
    mapEdgeInfoArray
  }
  //从文件中导入地图卡口信息
  private def getIntersection : ArrayBuffer[IntersectionPro] = {
    ArrayBuffer[IntersectionPro]()
  }
  //从文件导入地图边的信息
  private def getMapEdge : ArrayBuffer[MapEdgeInfo] = {
    val mapEdgeArray = new ArrayBuffer[MapEdgeInfo]()
    val mapEdgeFile = Source.fromFile(
      ticsInfo.getMapEdgeFilePath)
    val line = mapEdgeFile.getLines()
    for(i <- line){
      if(i.split("\t").length != 3){
        println("input error!!!")
      }else{
        val EdgeSourceNumber = i.split("\t")(0).toLong
        val EdgeDestNumber = i.split("\t")(1).toLong
        val EdgeLength = (i.split("\t")(2).toDouble * 1000).toLong
        mapEdgeArray += new MapEdgeInfo(EdgeSourceNumber, EdgeDestNumber, EdgeLength)
      }
    }
    mapEdgeArray
  }
  //从文件中导入地图顶点信息
  private def getMapVertex : ArrayBuffer[MapVertexInfo] = {
    val mapVertexArray = new ArrayBuffer[MapVertexInfo]()
    val mapVertexFile = Source.fromFile(
      ticsInfo.getMapVertexFilePath)
    val line = mapVertexFile.getLines()
    for(i <- line){
      if(i.split("\t").length != 2){
        println("input error!!!")
      }
      else{
        val VertexNumber = i.split("\t")(0).toLong
        val Latitude = i.split("\t")(1).split(" ")(0).toDouble
        val Longitude = i.split("\t")(1).split(" ")(1).toDouble
        mapVertexArray += new MapVertexInfo(VertexNumber, Latitude, Longitude)
      }
    }
    mapVertexArray
  }
}

package com.xy.lr.tics.properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{GraphLoader, Graph}
import org.apache.spark.graphx.VertexId

/**
 * Created by xylr on 15-3-9.
 */
class CarInfo extends java.io.Serializable{
  //车牌号
  private var carNumber : String = _
  //通过路口
  private var crossNumber : String = _
  //通过时间
  private var time : String = _
  //轨迹
  private var graph : String = _
  //通过方向
  private var direction : String = _
  def this(carNumber : String, crossNumber : String, time : String, graph : String
            ,direction : String){
    this()
    this.carNumber = carNumber
    this.crossNumber = crossNumber
    this.time = time
    this.graph = graph
    this.direction = direction
  }
  def getDirection : String = {
    direction
  }
  def getCarNumber : String = {
    carNumber
  }
  def getCrossNumber : String = {
    crossNumber
  }
  def getTime : String = {
    time
  }
  def getGraph : String = {
    graph
  }
}
/*
object CarInfo{
  def test(graph : Graph[Int,Int]): Unit ={
    println(graph.edges.collect()(0))
  }
  def main(args : Array[String]): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("ClientApp")
    val sc = new SparkContext(conf)

    val graph = GraphLoader.edgeListFile(sc,"/home/xylr/Data/edge.txt")
    graph.cache()
    val car = new CarInfo("123","1","20150309105030","","")
//    println(car.graph.vertices.collect()(1))
//    test(car.graph)
  }
}*/

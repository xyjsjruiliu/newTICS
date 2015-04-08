package com.xy.lr.scala.socket

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.{GraphLoader, Graph}

/**
 * Created by xylr on 15-3-9.
 */
class CarInfo extends java.io.Serializable{
  private var carNumber : String = _
  private var crossNumber : String = _
  private var time : String = _
  private var graph : String = _
  def this(carNumber : String, crossNumber : String, time : String, graph : String){
    this()
    this.carNumber = carNumber
    this.crossNumber = crossNumber
    this.time = time
    this.graph = graph
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
object CarInfo{
//  def test(graph : Graph[Int,Int]): Unit ={
//    println(graph.edges.collect()(0))
//  }
//  def main(args : Array[String]): Unit ={
//    val conf = new SparkConf().setMaster("local[2]").setAppName("ClientApp")
//    val sc = new SparkContext(conf)
//
//    val graph = GraphLoader.edgeListFile(sc,"/home/xylr/Data/edge.txt")
//    graph.cache()
//    val car = new CarInfo("123","1","20150309105030",graph)
//    println(car.graph.vertices.collect()(1))
//    test(car.graph)
//  }
}

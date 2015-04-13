package com.xy.lr.tics.spark

import com.xy.lr.tics.properties.{OldMessage, CarInfo, MapEdgeInfo, MapVertexInfo}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * Created by xylr on 15-3-3.
 */
object Client {
  def getMapVertexRDD : ArrayBuffer[MapVertexInfo] = {
    val mapVertexRDD = new ArrayBuffer[MapVertexInfo]()
    val mapVertexFile = Source.fromFile("/home/xylr/software/Data/guangzhou/cpp/vertex.txt")
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
  def getMapEdgeRDD : ArrayBuffer[MapEdgeInfo] = {
    val mapEdgeRDD = new ArrayBuffer[MapEdgeInfo]()
    val mapEdgeFile = Source.fromFile("/home/xylr/software/Data/guangzhou/cpp/edge1.txt")
    val line = mapEdgeFile.getLines()
    for(i <- line){
      if(i.split("\t").length != 3){
        println("input error!!!")
      }else{
        val EdgeSourceNumber = i.split("\t")(0).toLong
        val EdgeDestNumber = i.split("\t")(1).toLong
        val EdgeLength = (i.split("\t")(2).toDouble * 1000).toLong
        mapEdgeRDD += new MapEdgeInfo(EdgeSourceNumber, EdgeDestNumber, EdgeLength)
      }
    }
    mapEdgeRDD
  }
  def getMonitorRDD : ArrayBuffer[String] = {
    val monitorRDD = new ArrayBuffer[String]()
    val monitorFile = Source.fromFile("/home/xylr/project/112")
    val line = monitorFile.getLines()
    for(i <- line){
      monitorRDD += i
    }
    monitorRDD
  }
  def saveMessage() : ArrayBuffer[String] = {
    val message = new ArrayBuffer[String]()
    message
  }
  def getMessage(path : String) : ArrayBuffer[String] = {
    val message = new ArrayBuffer[String]()
    val messageFile = Source.fromFile(path)
    val line = messageFile.getLines()
    for(i <- line){
      message += i
    }
    message
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("ClientApp")
    val sc = new SparkContext(conf)

    val MonitorRDD = sc.parallelize(getMonitorRDD)
    MonitorRDD.cache()
    val MapEdgeRDD = sc.parallelize(getMapEdgeRDD)
    MapEdgeRDD.cache()
    val MapVertexRDD = sc.parallelize(getMapVertexRDD)
    MapVertexRDD.cache()


    @transient var CarRDD = sc.parallelize(new ArrayBuffer[CarInfo]())
    CarRDD.cache()
    @transient var oldMessageRDD = sc.parallelize(new ArrayBuffer[OldMessage]())
    oldMessageRDD.cache()
    @transient var message = new ArrayBuffer[String]()

    val client = new DeZhouClient("127.0.0.1",9999)
    @transient var i = 0
    while(true){
      i = i + 1
      val m = client.getMsg
      message += m
      println(m)
      if(message.length % 10 == 0)
      {
        //need use thread
        val notUseMessageRDD = getNotUseMessageRDD(sc , message , oldMessageRDD , CarRDD)
        //make the graph
        CarRDD = makeTheGraph(sc , message , CarRDD , oldMessageRDD)
        //update the old rdd
        oldMessageRDD = notUseMessageRDD

        oldMessageRDD.collect().foreach(x => {
          println("OldMessageRDD start")
          println(x.getCarNumber)
          println(x.getOldMessageRoute)
          println("OldMessageRDD Stop!")
        })
        val carRDD : RDD[(Int,String)] = CarRDD.map( x => {
          val carNumber = x.getCarNumber.toInt
          val length = x.getGraph.toString
          (carNumber,length)
        })
        oldMessageRDD.saveAsTextFile("/home/xylr/Data/old" + i)
        notUseMessageRDD.collect().foreach(x => {
          println(x.getCarNumber)
          println(x.getOldMessageRoute)
        })

//        CarRDD.collect().foreach( x => {
//          println("CarRDD start")
//          println(x.getCarNumber)
//          println(x.getCrossNumber)
//          println(x.getGraph)
//          println(x.getTime)
//          println("CarRDD stop!")
//        })
        message = new ArrayBuffer[String]()
      }
    }
  }
  def getNotUseMessageRDD(sc : SparkContext, newMessage : ArrayBuffer[String],
                          oldMessageRDD : RDD[OldMessage], CarRDD : RDD[CarInfo]): RDD[OldMessage] ={
    //oldRDD
    val oldRDD = oldMessageRDD.map(x => {
        val s = x.getOldMessageRoute
        (x.getCarNumber.toInt,s)
    })
    //carRDD
    val carRDD : RDD[(Int,String)] = CarRDD.map( x => {
      val carNumber = x.getCarNumber.toInt
      val length = x.getGraph.toString
      (carNumber,length)
    })
    //最新流数据的RDD
    val newMessageRDD = sc.parallelize(newMessage)
    //newRDD
    val newRDD : RDD[(Int,String)] = newMessageRDD.map( x => {
      val carNumber = x.split(",")(1)
      if(x.split(",").length != 5)
        //error input
        (0,"0")
      else (carNumber.toInt,x)
    }).groupByKey().mapValues( x => {
      @transient var tmp = ""
      x.map( y => tmp += y.split(",")(2) + "," + y.split(",")(3) + ":")
      tmp.substring(0,tmp.length-1)
    }).filter( x => {
      if(x._1 == 0) false
      else true
    })
    //过滤到长度大于1的
    val filterRDD = newRDD.filter( x =>
      if(x._2.split(":").length <= 1) true
      else false
    )
    if(oldRDD.count() == 0){
      val newRDDJoinCarRDD = filterRDD.join(carRDD)
      val newRDDJoinCarRDDSub = filterRDD.subtractByKey(newRDDJoinCarRDD)

      val result = newRDDJoinCarRDDSub.map(x => {
        val car = new OldMessage(x._1.toString,x._2)
        car
      })
      result
    }
    else{
      val joinRDD = newRDD.join(oldRDD)
      val sub1 = oldRDD.subtractByKey(joinRDD)
      val sub2 = newRDD.subtractByKey(joinRDD)
      val un = sub1.union(sub2)

      val newRDDJoinCarRDD = un.join(carRDD)
      val newRDDJoinCarRDDSub = un.subtractByKey(newRDDJoinCarRDD)

      val result = newRDDJoinCarRDDSub.filter( x =>
        if(x._2.split(":").length <= 1) true
        else false
      ).map(x => {
        val car = new OldMessage(x._1.toString,x._2)
        car
      })
      result
    }
  }

  def makeTheGraph(sc : SparkContext, newMessage : ArrayBuffer[String], CarRDD : RDD[CarInfo],
                 oldMessageRDD : RDD[OldMessage]): RDD[CarInfo] ={
    val oldRDD : RDD[(Int,String)] = oldMessageRDD.map(x => {
        val s = x.getOldMessageRoute
        (x.getCarNumber.toInt,s)
    })
    val newMessageRDD = sc.parallelize(newMessage)
    val newRDD : RDD[(Int,String)] = newMessageRDD.map( x => {
      val carNumber = x.split(",")(1)
      if(x.split(",").length != 5)
      //error input
        (0,"0")
      else (carNumber.toInt,x)
    }).groupByKey().mapValues( x => {
      @transient var tmp = ""
      x.map( y => tmp += y.split(",")(2) + "," + y.split(",")(3) + ":")
      tmp.substring(0,tmp.length-1)
    }).filter( x => {
      if(x._1 == 0) false
      else true
    })
    val carRDD : RDD[(Int,String)] = CarRDD.map( x => {
      val carNumber = x.getCarNumber.toInt
      val length = x.getGraph.toString
      (carNumber,length)
    })

//    val oldRDDJoinNewRDD = oldRDD.join(newRDD)

    if(oldRDD.count() == 0){
      val filterRDD = newRDD.filter( x =>
        if(x._2.split(":").length <= 1) false
        else true
      )
      if(CarRDD.count() == 0){
        val ca = filterRDD.map( x => {
          val carNumber = x._1.toString
          val tmp = x._2.split(":")
          val cnt = tmp(tmp.length-1)
//          println(carNumber + "," + cnt)
          val car = new CarInfo(carNumber,cnt.split(",")(1),cnt.split(",")(0),x._2,"")
          car
        })
        ca
//         ca.collect().foreach( x=> {
//           println(x.getCarNumber)
//           println(x.getCrossNumber)
//          println(x.getTime)
//           println(x.getGraph)
//         })
      }
      else{
        val carRDDAndfilterRDD = carRDD.join(newRDD)
        val ca = carRDDAndfilterRDD.map( x => {
          println(x._1 + "\t" + x._2._1 + "\t" + x._2._2 +"\n")
          val tmp = x._2._1 + ":" + x._2._2
          (x._1, tmp)
        })
        val t = newRDD.subtractByKey(carRDDAndfilterRDD)
        val t1 = carRDD.subtractByKey(carRDDAndfilterRDD)
        val xx = t.union(ca).union(t1).filter(x =>
          if(x._2.split(":").length <= 1) false
          else true)
        val cc = xx.map( y => {
          val carNumber = y._1.toString
          val tmp = y._2.split(":")
          val cnt = tmp(tmp.length-1)
          println(carNumber + "," + cnt)
          val car = new CarInfo(carNumber,cnt.split(",")(1),cnt.split(",")(0),y._2,"")
          car
        })
        cc
      }
    }
    else{
      if(CarRDD.count() == 0){
        val unionRDD = newRDD.join(oldRDD)
        val ca = unionRDD.map( x => {
          println(x._1 + "\t" + x._2._1 + "\t" + x._2._2 +"\n")
          val tmp = x._2._2 + ":" + x._2._1
          (x._1, tmp)
        })
        val t = newRDD.subtractByKey(ca)
        val xx = t.union(ca).filter(x =>
          if(x._2.split(":").length <= 1) false
          else true)
        val cc = xx.map( y => {
          val carNumber = y._1.toString
          val tmp = y._2.split(":")
          val cnt = tmp(tmp.length-1)
          println(carNumber + "," + cnt)
          val car = new CarInfo(carNumber,cnt.split(",")(1),cnt.split(",")(0),y._2,"")
          car
        })
        cc
      }
      else{
        val newAndOldUnionRDD = newRDD.join(oldRDD).map( x => {
          val tmp = x._2._2 + ":" + x._2._1
          (x._1 , tmp)
        })
        val newRDDSub = newRDD.subtractByKey(newAndOldUnionRDD)
        val newJoinOldRDD = newRDDSub.union(newAndOldUnionRDD)

        val newAndCarUnionRDD = carRDD.join(newJoinOldRDD).map( x => {
          val tmp = x._2._1 + ":" + x._2._2
          (x._1 , tmp)
        })
        val subNewRDD = newJoinOldRDD.subtractByKey(newAndCarUnionRDD)
        val subCarRDD = carRDD.subtractByKey(newAndCarUnionRDD)
        val newJoinCarRDD = subNewRDD.union(subCarRDD).union(newAndCarUnionRDD)

        val ca = newJoinCarRDD.filter( x => {
          if(x._2.split(":").length <= 1) false
          else true
        })
        val cc = ca.map( y => {
          val carNumber = y._1.toString
          val tmp = y._2.split(":")
          val cnt = tmp(tmp.length-1)
          println(carNumber + "," + cnt)
          val car = new CarInfo(carNumber,cnt.split(",")(1),cnt.split(",")(0),y._2,"")
          car
        })
        cc
      }
    }
  }
  def getTime(x : (Long,String)) : String = {
    val list = x._2.split(":")
    list(list.length - 1).split(",")(0)
  }
  def getCrossingNumber(x : (Long,String)) : String = {
    val list = x._2.split(":")
    list(list.length - 1).split(",")(1)
  }

  /*def makeGraph(x : (Long,String), sc : SparkContext) : Graph[String,String] = {
    val vertex = new ArrayBuffer[(Long, String)]()
    val edge = new ArrayBuffer[Edge[String]]()
//    val carNum = x._1
    val list = x._2.split(":")
    for (i <- 0 until list.length - 1) {
      if (list(i).split(",").length != 2 && list(i + 1).split(",").length != 2) {
        println("input error!!!")
      } else {
        vertex += (list(i).split(",")(1).toLong -> list(i).split(",")(0))
        edge += Edge(list(i).split(",")(1).toLong, list(i + 1).split(",")(1).toLong, "1")
      }
      //          println(list(i) + "\t" + list(i+1))
    }
    vertex += (list(list.length - 1).split(",")(1).toLong -> list(list.length - 1).split(",")(0))
    val users: RDD[(VertexId, String)] = sc.parallelize(vertex)
    val relation: RDD[Edge[String]] = sc.parallelize(edge)
    val default = "default"
    val graph = Graph(users, relation, default)
    graph.cache()
    println(graph.vertices.collect() + "\t" + graph.edges.collect())
    graph
  }*/
}

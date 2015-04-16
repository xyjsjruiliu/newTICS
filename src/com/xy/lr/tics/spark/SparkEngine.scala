package com.xy.lr.tics.spark

import java.text.SimpleDateFormat
import java.util.Date

import com.xy.lr.tics.properties._
import com.xy.lr.tics.spark.sql.{CarCaseClass, Person}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ArrayBuffer

//import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by xylr on 15-4-8.
 */
class SparkEngine extends Thread{
  private var rdd : RDD[Int] = _
  private var sparkConf : SparkConf = _
  private var sparkContext : SparkContext = _
  private var sqlContext : SQLContext = _
  private var jsonFileExample : SchemaRDD = _
  private var people : RDD[String] = _
  private var CarRDD : RDD[CarInfo] = _
  private var oldMessageRDD : RDD[OldMessage] = _
  private var message : ArrayBuffer[String] = _
  private var ticsInfo : TICSInfo = _
  private var deZhouClient : DeZhouClientJava = _
  private var mapVertexRDD : RDD[MapVertexInfo] = _
  private var mapEdgeRDD : RDD[MapEdgeInfo] = _
  private var intersectionpProRDD : RDD[IntersectionPro] = _
  private var engineProperty : EngineProperty = _


//  conf = new SparkConf().setMaster("local[2]").setAppName("defaultApp")
//  sc = new SparkContext(conf)

  def this(MasterUrl : String, AppName : String, path : String){
    this
    //SparkConf
    sparkConf = new SparkConf().setMaster(MasterUrl).setAppName(AppName)
    //SparkContext
    sparkContext = new SparkContext(sparkConf)
    //SparkSQlContext
    sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    //配置文件
    ticsInfo = new TICSInfo(path)
//    import sqlContext.createSchemaRDD
    engineProperty = new EngineProperty(path)
  }
  def sql(query : String): String ={
    ""
  }
  def getCarGraph(carNumber : String) : String = {
    val carRDD = CarRDD.map( x => {
      new CarCaseClass(x.getCarNumber, x.getCrossNumber, x.getTime,
      x.getGraph, x.getDirection)
    })
    val carSchemaRDD = sqlContext.createSchemaRDD(carRDD)
    carSchemaRDD.registerTempTable("car")
    val sqlResult = sqlContext.sql("select carNumber,graph from car")

    val carNumberAndGraph = sqlResult.filter( x => {
      println("###############\n\n" + carNumber + ":" +x(0) + "\n\n#############")
      if(x(0).equals(carNumber)) true
      else false
    })
    val graph = carNumberAndGraph.map( x => {
      println("+++++++++++++++++++++++\n" + x + "\n+++++++++++++++++++++")
      x(1).toString
    }).collect()

    /*查询结果*/
    if(graph.length == 0){
      "no result"
    }else{
      graph(0)
    }
  }
  def initSparkEngine(): Unit ={
    rdd = sparkContext.parallelize(List(1))

    people = sparkContext.textFile(
      "/home/xylr/software/my_spark/spark-1.0.1-bin-2.4.0/examples/src/main/resources/people.txt")
    //数据初始化
    CarRDD = sparkContext.parallelize(new ArrayBuffer[CarInfo]())
    CarRDD.cache()
    oldMessageRDD = sparkContext.parallelize(new ArrayBuffer[OldMessage]())
    oldMessageRDD.cache()
    message = new ArrayBuffer[String]()
    //jsonFileExample = sqlContext.jsonFile("/home/xylr/software/spark-1.3.0/examples/src/main/resources/people.json")
    //jsonFileExample.registerAsTable("people")
    //sqlContext.cacheTable("people")

    //连接卡口服务器
    deZhouClient = new DeZhouClientJava(ticsInfo.getDeZhouServerUrl,
      ticsInfo.getDeZhouServerPort.toInt)

    engineProperty.initEngineProperty()

    mapVertexRDD = sparkContext.parallelize(engineProperty.getMapVertexInfoArray)
    mapEdgeRDD = sparkContext.parallelize(engineProperty.getMapEdgeInfoArray)
    intersectionpProRDD = sparkContext.parallelize(engineProperty.getIntersectionProArray)
  }
  override def run(): Unit ={
    println("start [ SparkEngine ] at " + getCurrentTime)
    initSparkEngine()

    @transient var i = 0
    while(true){
      i = i + 1
      //获得卡口信息
      val m = deZhouClient.getMsg
      message += m
      println(m)
      //每10份作为一个数据块
      if(message.length % 10 == 0)
      {
        //need use thread
        val notUseMessageRDD = getNotUseMessageRDD(sparkContext , message , oldMessageRDD , CarRDD)
        //make the graph
        CarRDD = makeTheGraph(sparkContext , message , CarRDD , oldMessageRDD)
        //update the old rdd
        oldMessageRDD = notUseMessageRDD

        /*oldMessageRDD.collect().foreach(x => {
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
        })*/
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
  def getRDD = {
    rdd
  }
  def getNameOfJsonFile : String = {
    val name = sqlContext.sql("select name from people")
    name.collect()(1).toString()
  }
  def getPeoplesName : String = {
    val peopleTable = people.map(_.split(",")).map(p =>
      Person(p(0), p(1).trim.toInt))
    //创建一个新的SchemaRDD
    val table = sqlContext.createSchemaRDD(peopleTable)
    //注册成为一个叫people的sql表
    table.registerTempTable("people")
    //查询name
    val name = sqlContext.sql("select name from people")
    name.collect()(1).toString()
  }
  def getCurrentTime : String = {
    val date = new Date()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val times = df.format(date)
    times
  }

}
/*
object SparkEngine {
  def main(args : Array[String]): Unit ={
    val sparkEngine = new SparkEngine("local[2]","lr")
    sparkEngine.start()
  }
}*/
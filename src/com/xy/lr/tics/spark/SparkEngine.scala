package com.xy.lr.tics.spark

import java.text.SimpleDateFormat
import java.util.Date

import com.xy.lr.tics.properties._
import com.xy.lr.tics.spark.sql.{CarCaseClass, Person}
import org.apache.spark.sql.{Row, SQLContext, SchemaRDD}

import scala.collection.mutable.ArrayBuffer

//import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Created by xylr on 15-4-8.
 * sparkEngine
 */
class SparkEngine extends Thread{
  //测试时候使用的rdd
  private var rdd : RDD[Int] = _
  //spark 配置信息
  private var sparkConf : SparkConf = _
  //spark 主入口
  private var sparkContext : SparkContext = _
  //spark sql 主入口
  private var sqlContext : SQLContext = _
  //测试时候使用的json文件
  private var jsonFileExample : SchemaRDD = _
  //测试时候使用
  private var people : RDD[String] = _
  //车辆信息RDD
  private var CarRDD : RDD[CarInfo] = _
  //每一轮处理车辆信息时,略过的车辆信息RDD
  private var oldMessageRDD : RDD[OldMessage] = _
  //从卡口服务器那边接收的车辆信息
  private var message : ArrayBuffer[String] = _
  //配置文件
  private var ticsInfo : TICSInfo = _
  //卡口服务器的客户端
  private var deZhouClient : DeZhouClientJava = _
  //地图顶点信息RDD
  private var mapVertexRDD : RDD[MapVertexInfo] = _
  //地图边信息RDD
  private var mapEdgeRDD : RDD[MapEdgeInfo] = _
  //路口信息RDD
  private var intersectionProRDD : RDD[IntersectionPro] = _
  //SparkEngine 启动时默认导入的信息
  private var engineProperty : EngineProperty = _
  //黑名单RDD
  private var blackListRDD : RDD[BlackList] = _

  //构造函数,初始化SparkConf,parkContext,SQLContext,配置文件
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
    //初始信息
    engineProperty = new EngineProperty(path)
  }

  //通过 sql代码 查询 （暂时还没有完成）
  def sql(query : String): String ={
    val carRDD = CarRDD.map( x => {
      new CarCaseClass(x.getCarNumber, x.getCrossNumber, x.getTime,
        x.getGraph, x.getDirection)
    })
    val carSchemaRDD = sqlContext.createSchemaRDD(carRDD)
    carSchemaRDD.registerTempTable("car")
    val sqlResult = sqlContext.sql(query).collect()
    ""
  }

  //通过车牌号查询轨迹
  def getCarGraph(carNumber : String) : String = {
    val carRDD = CarRDD.map( x => {
      new CarCaseClass(x.getCarNumber, x.getCrossNumber, x.getTime,
      x.getGraph, x.getDirection)
    })
    val carSchemaRDD = sqlContext.createSchemaRDD(carRDD)
    //将CarRDD注册成为car表
    carSchemaRDD.registerTempTable("car")
    //执行sql查询
    val sqlResult = sqlContext.sql("select carNumber,graph from car where carNumber = \"" + carNumber + "\"")

    //解析车辆轨迹
    val carNumberAndGraph = sqlResult.filter( x => {
      //println("###############\n\n" + carNumber + ":" +x(0) + "\n\n#############")
      if(x(0).equals(carNumber)) true
      else false
    })
    val graph = carNumberAndGraph.map( x => {
      //println("+++++++++++++++++++++++\n" + x + "\n+++++++++++++++++++++")
      x(1).toString
    }).collect()

    /*查询结果*/
    if(graph.length == 0){
      "no result"
    }else{
      graph(0)
    }
  }

  //初始化SparkEngine
  def initSparkEngine(): Unit ={
    rdd = sparkContext.parallelize(List(1))

    /*people = sparkContext.textFile(
      "/home/xylr/software/my_spark/spark-1.0.1-bin-2.4.0/examples/src/main/resources/people.txt")*/
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

    //初始化默认导入信息
    engineProperty.initEngineProperty()
    mapVertexRDD = sparkContext.parallelize(engineProperty.getMapVertexInfoArray)
    mapEdgeRDD = sparkContext.parallelize(engineProperty.getMapEdgeInfoArray)
    intersectionProRDD = sparkContext.parallelize(engineProperty.getIntersectionProArray)
    blackListRDD = sparkContext.parallelize(engineProperty.getBlackListArray)
  }

  //查询黑名单列表
  def sqlBlackList(): String ={
    //获取黑名单车牌号
    val bl : RDD[Int] = blackListRDD.map( x => {
      val carNumber = x.getCarNumber.toInt
      //val graph = x.getCarGraph.getGraph.toString
      carNumber
    })
    @transient var list : String = ""
    bl.collect().map( i => {
      list = list + i + ":"
    })
    list = list.substring(0, list.length - 1)
    //返回黑名单
    list
  }

  //查询黑名单车辆轨迹
  def sqlGraphByBlackList(carNumber : String) : String = {
    getCarGraph(carNumber)
  }

  //更新黑名单RDD
  private def updateBlackList(blacklistRDD : RDD[BlackList],
                      CarRDD : RDD[CarInfo]): RDD[BlackList] ={
    //车辆信息RDD
    val carRDD : RDD[(Int,CarInfo)] = CarRDD.map( x => {
      val carNumber = x.getCarNumber.toInt
//      val length = x.getGraph.toString
      (carNumber,x)
    })
    //黑名单车辆信息RDD
    val blRDD : RDD[(Int,CarInfo)] = blacklistRDD.map( x => {
      val carNumber = x.getCarNumber.toInt
//      val graph = x.getCarGraph.getGraph.toString
      (carNumber, x.getCarGraph)
    })
    val joinRDD = blRDD.join(carRDD)
    val subRDD = blRDD.subtractByKey(joinRDD)

    val join = joinRDD.map( x => {
      (x._1, x._2._2)
    })
    val newBlackListRDD = join.union(subRDD).map( x => {
      new BlackList(x._1.toString, x._2)
    })
    //更新后的黑名单
    newBlackListRDD
  }

  //主函数
  override def run(): Unit ={
    println("start [ SparkEngine ] at " + getCurrentTime)

    //初始化SparkEngine
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

        blackListRDD = updateBlackList(blackListRDD, CarRDD)
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

  //没有使用到的车辆信息
  private def getNotUseMessageRDD(sc : SparkContext, newMessage : ArrayBuffer[String],
                          oldMessageRDD : RDD[OldMessage], CarRDD : RDD[CarInfo]): RDD[OldMessage] ={
    //oldRDD
    val oldRDD = parseOldMessageRDDTooldRDD(oldMessageRDD)
    //carRDD
    val carRDD : RDD[(Int,String)] = parseCarRDDTocarRDD(CarRDD)
    //最新流数据的RDD
    val newMessageRDD = sc.parallelize(newMessage)
    //newRDD
    val newRDD : RDD[(Int,String)] = parseNewMessageRDDToNewRDD(newMessageRDD)

    //过滤到长度大于1的
    val filterRDD = filterLength(newRDD)

    //如果oldRDD之前不存在
    if(oldRDD.count() == 0){
      //如果newRDD中存在有数据在carRDD中出现,需要删除这一部分数据
      val newRDDJoinCarRDD = filterRDD.join(carRDD)
      val newRDDJoinCarRDDSub = filterRDD.subtractByKey(newRDDJoinCarRDD)

      val result = parseMessageRDDToOldMessageRDD(newRDDJoinCarRDDSub)
      result
    }
    else{
      val joinRDD = newRDD.join(oldRDD)
      val sub1 = oldRDD.subtractByKey(joinRDD)
      val sub2 = newRDD.subtractByKey(joinRDD)
      val un = sub1.union(sub2)

      val newRDDJoinCarRDD = un.join(carRDD)
      val newRDDJoinCarRDDSub = un.subtractByKey(newRDDJoinCarRDD)

      val result = parseMessageRDDToOldMessageRDD(filterLength(newRDDJoinCarRDDSub))
      result
    }
  }

  //RDD[(Int, String)]类型数据转换成为RDD[OldMessage]
  private def parseMessageRDDToOldMessageRDD(pMRTOMRrdd : RDD[(Int, String)]) : RDD[OldMessage] = {
    val result = pMRTOMRrdd.map(x => {
      val car = new OldMessage(x._1.toString, x._2)
      car
    })
    result
  }

  //RDD[String]类型的数据转换成为RDD[(Int, String)]
  private def parseNewMessageRDDToNewRDD(nMD : RDD[String]) : RDD[(Int, String)] = {
    val newRDD : RDD[(Int,String)] = nMD.map( x => {
      val carNumber = x.split(",")(1)
      if(x.split(",").length != 5)
      //error input
        (0,"0")
      else (carNumber.toInt,x)
    }).groupByKey().mapValues( x => {
      @transient var tmp = ""
      x.map( y => tmp += y.split(",")(2) + "," + y.split(",")(3) + ":")
      tmp.substring(0,tmp.length-1)
    }).filter( x => { //过滤error input
      if(x._1 == 0) false
      else true
    })
    newRDD
  }

  //RDD[CarInfo]类型数据转换成为RDD[(Int, String)]
  private def parseCarRDDTocarRDD(CarRDD : RDD[CarInfo]) : RDD[(Int, String)] = {
    val carRDD : RDD[(Int,String)] = CarRDD.map( x => {
      val carNumber = x.getCarNumber.toInt
      val length = x.getGraph.toString
      (carNumber,length)
    })
    carRDD
  }
  //RDD[OldMessage]类型数据转换成为RDD[(Int, String)]
  private def parseOldMessageRDDTooldRDD(pOMRToOR : RDD[OldMessage]) : RDD[(Int, String)] = {
    val oldRDD : RDD[(Int,String)] = pOMRToOR.map(x => {
      val s = x.getOldMessageRoute
      (x.getCarNumber.toInt,s)
    })
    oldRDD
  }

  //过滤掉长度小于1的数据
  private def filterLength(flnewRDD : RDD[(Int, String)]) : RDD[(Int, String)] = {
    val flfilterRDD = flnewRDD.filter( x => if(x._2.split(":").length <= 1) false else true)
    flfilterRDD
  }

  //将一个类型为 RDD[(Int, String)] 的对象转换成为 RDD[CarInfo] 对象
  private def rddToCarInfoRDD(rdd : RDD[(Int, String)]) : RDD[CarInfo] = {
    val ca = rdd.map( x => {
      val carNumber = x._1.toString
      val tmp = x._2.split(":")
      val cnt = tmp(tmp.length-1)
      val car = new CarInfo(carNumber,cnt.split(",")(1),cnt.split(",")(0),x._2,"")
      car
    })
    ca
  }

  //合并newRDD, carRDD
  private def mergeNewRDDAndCarRDD(mNRCRcarRDD : RDD[(Int, String)], mNRCRnewRDD : RDD[(Int, String)]
                                    ) : RDD[(Int, String)] = {
    val carRDDAndfilterRDD = mNRCRcarRDD.join(mNRCRnewRDD)
    val ca = carRDDAndfilterRDD.map( x => {
      println(x._1 + "\t" + x._2._1 + "\t" + x._2._2 +"\n")
      val tmp = x._2._1 + ":" + x._2._2
      (x._1, tmp)
    })
    val t = mNRCRnewRDD.subtractByKey(carRDDAndfilterRDD)
    val t1 = mNRCRcarRDD.subtractByKey(carRDDAndfilterRDD)
    val xx = t.union(ca).union(t1).filter(x =>
      if(x._2.split(":").length <= 1) false
      else true)
    xx
  }

  //合并newRDD, oldRDD
  private def mergeNewRDDAndOldRDD(mNRORoldRDD : RDD[(Int, String)], mNRORnewRDD : RDD[(Int, String)]
                                    ) : RDD[(Int, String)] = {
    val unionRDD = mNRORnewRDD.join(mNRORoldRDD)
    val ca = unionRDD.map( x => {
      println(x._1 + "\t" + x._2._1 + "\t" + x._2._2 +"\n")
      val tmp = x._2._2 + ":" + x._2._1
      (x._1, tmp)
    })
    val t = mNRORnewRDD.subtractByKey(ca)
    val xx = t.union(ca).filter(x =>
      if(x._2.split(":").length <= 1) false
      else true)
    xx
  }

  //计算车辆轨迹
  private def makeTheGraph(sc : SparkContext, newMessage : ArrayBuffer[String], CarRDD : RDD[CarInfo],
                   oldMessageRDD : RDD[OldMessage]): RDD[CarInfo] ={
    //解析OldMessageRDD
    val oldRDD : RDD[(Int,String)] = parseOldMessageRDDTooldRDD(oldMessageRDD)

    val newMessageRDD = sc.parallelize(newMessage)
    //解析newMessageRDD
    val newRDD : RDD[(Int,String)] = parseNewMessageRDDToNewRDD(newMessageRDD)

    //解析CarRDD
    val carRDD : RDD[(Int,String)] = parseCarRDDTocarRDD(CarRDD)

    //如果没有old
    if(oldRDD.count() == 0){
      //过滤掉长度小于等于1的车辆信息
      val filterRDD = filterLength(newRDD)
      if(CarRDD.count() == 0){
        val ca = rddToCarInfoRDD(filterRDD)
        ca
      }
      else{
        val xx = mergeNewRDDAndCarRDD(carRDD, newRDD)
        val cc = rddToCarInfoRDD(xx)
        cc
      }
    }
    //如果有old
    else{
      //如果原始数据库没有车辆信息
      if(CarRDD.count() == 0){
        val xx = mergeNewRDDAndOldRDD(oldRDD, newRDD)
        val cc = rddToCarInfoRDD(xx)
        cc
      }
      //如果原始数据库中有车辆信息
      else{
        val ca = mergeCarRDDAndNewRDDAndOldRDD(carRDD, newRDD, oldRDD)
        val cc = rddToCarInfoRDD(ca)
        cc
      }
    }
  }

  //合并carRDD, newRDD, oldRDD
  private def mergeCarRDDAndNewRDDAndOldRDD(mCRANRAORcarRDD : RDD[(Int, String)],
                                            mCRANRAORnewRDD : RDD[(Int, String)],
                                            mCRANRAORoldRDD : RDD[(Int, String)]) : RDD[(Int, String)] = {
    val newAndOldUnionRDD = mCRANRAORnewRDD.join(mCRANRAORoldRDD).map( x => {
      val tmp = x._2._2 + ":" + x._2._1
      (x._1 , tmp)
    })
    val newRDDSub = mCRANRAORnewRDD.subtractByKey(newAndOldUnionRDD)
    val newJoinOldRDD = newRDDSub.union(newAndOldUnionRDD)

    val newAndCarUnionRDD = mCRANRAORcarRDD.join(newJoinOldRDD).map( x => {
      val tmp = x._2._1 + ":" + x._2._2
      (x._1 , tmp)
    })
    val subNewRDD = newJoinOldRDD.subtractByKey(newAndCarUnionRDD)
    val subCarRDD = mCRANRAORcarRDD.subtractByKey(newAndCarUnionRDD)
    val newJoinCarRDD = subNewRDD.union(subCarRDD).union(newAndCarUnionRDD)

    val ca = newJoinCarRDD.filter( x => {
      if(x._2.split(":").length <= 1) false
      else true
    })
    ca
  }

  //获取轨迹时间
  private def getTime(x : (Long,String)) : String = {
    val list = x._2.split(":")
    list(list.length - 1).split(",")(0)
  }

  //获取通过路口
  private def getCrossingNumber(x : (Long,String)) : String = {
    val list = x._2.split(":")
    list(list.length - 1).split(",")(1)
  }

  //获取rdd
  def getRDD = {
    rdd
  }

  //
  def getNameOfJsonFile : String = {
    val name = sqlContext.sql("select name from people")
    name.collect()(1).toString()
  }

  //
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

  //获取当前时间
  private def getCurrentTime : String = {
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
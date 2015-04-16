package com.xy.lr.tics.properties

import java.io.IOException

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.graphx._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by xylr on 15-4-15.
 */
class ShortPaths {
  def getShortPathExample(hbaseinfo : HBaseInfo, source : String, dest : String) : String = {
    val configuration = HBaseConfiguration.create()
    configuration.addResource("/home/xylr/software/hbase-0.99.0/conf/hbase-site.xml")

    @transient var usersTable: HTable = null
    try {
      usersTable = new HTable(configuration, hbaseinfo.getControlerInHBase)
    }
    catch {
      case e: IOException => {
        e.printStackTrace()
        System.out.println("unable load HTable by getShortPathExample()")
      }
    }
    val g: Get = new Get(Bytes.toBytes(source+"\t"+dest))
    g.addColumn(Bytes.toBytes(hbaseinfo.getINFO), Bytes.toBytes(hbaseinfo.getShortPath))
    @transient var r: Result = null
    @transient var temp = ""
    try {
      val r: Result = usersTable.get(g)
      val b: Array[Byte] = r.getValue(Bytes.toBytes(hbaseinfo.getINFO), Bytes.toBytes(hbaseinfo.getShortPath))
      temp = Bytes.toString(b)
      usersTable.close()
    }
    catch {
      case e: IOException => {
        println("unable get result by getShortPathExample()")
        e.printStackTrace()
      }
    }
    temp
  }
  def noBelongControlerAndSourceDest(control: ArrayBuffer[String], edge: VertexId,
                                     source: String, dest: String): Boolean = {
    var flag: Boolean = true
    control.map(x =>
      if (edge == x.toLong)
        if (edge == source.toLong || edge == dest.toLong) {} else flag = false)
    flag
  }

  def getShortPath(graph : Graph[Int,Int], source: String, dest: String, control: ArrayBuffer[String]
                    ): String = {
    /*println(graph.vertices.take(10))*/
    //source vertex
    val sourceId: VertexId = source.toLong

    /*subgraph noBelongControlerAndSourceDest*/
    @transient var subgraph : Graph[Int,Int] = null
    if(control.length == 0){
      subgraph = graph
    }
    else{
      subgraph = graph.subgraph(vpred = (vid, x) => {
        if (noBelongControlerAndSourceDest(control, vid, source, dest)) true else false
      })
    }

    //the initial function
    val initialGraph = subgraph.mapVertices(
      (id, attr) =>
        if (id == sourceId)
          (0.0, ArrayBuffer[VertexId](sourceId))
        else
          (Double.PositiveInfinity, ArrayBuffer[VertexId]())
    )

    val sssp = initialGraph.pregel(
      //initialMsg the message each vertex will receive at the on the first iteration
      initialMsg = (Double.PositiveInfinity, ArrayBuffer[VertexId]()),
      //the direction of edges
      activeDirection = EdgeDirection.Out)(
        //user-defined vertex program
        (id, dist, newDist) => {
          //println("vertex function")
          if (dist._1 > newDist._1) newDist else dist
        },
        //sendMsg
        triplet => {
          if (triplet.srcAttr._1 + triplet.attr < triplet.dstAttr._1) {
            //println((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2)))
            Iterator((triplet.dstId,
              (triplet.srcAttr._1 + triplet.attr,
                ArrayBuffer[VertexId]() ++= triplet.srcAttr._2 += triplet.dstId)))
          } else
            Iterator.empty
        },
        //mergeMsg
        (a, b) => if (a._1 > b._1) b else a
      )
    var shortPath = new String("")
    val sp = sssp.vertices.collect()
    sp.map(x => {
      if (x._1.toString.equals(dest)) {
        x._2._2.map(y => {
          shortPath = shortPath + y.toString + "->"
        })
      }
    })
    //println(sssp.vertices.collect().mkString("\n"))
    shortPath = shortPath.substring(0, shortPath.lastIndexOf("->"))
    shortPath
  }
}

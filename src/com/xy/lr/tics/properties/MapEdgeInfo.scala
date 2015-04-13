package com.xy.lr.tics.properties

/**
 * Created by xylr on 15-3-9.
 */
class MapEdgeInfo extends java.io.Serializable{
  private var sourceID : Long = _
  private var destID : Long = _
  private var length : Long = _
  def this(source : Long, dest : Long, len : Long){
    this()
    sourceID = source
    destID = dest
    length = len
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
}

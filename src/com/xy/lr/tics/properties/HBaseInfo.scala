package com.xy.lr.tics.properties

import java.io.{IOException, FileInputStream, BufferedInputStream, File}
import java.util.Properties

/**
 * Created by xylr on 15-4-15.
 */
class HBaseInfo extends java.io.Serializable{
  private var p : Properties = _
  private var Path : String = _
  private var GuangZhouMapInHBase : String = _
  private var GuangZhouMapLongitude : String = _
  private var HBaseConfPath : String = _
  private var CrossingNumber : String = _
  private var TransitTime : String = _
  private var GuangZhouMapLatitude : String = _
  private var GuangZhouMapGeographicLocation : String = _
  private var HBaseShortPaths : String = _
  private var EdgeFileExample : String = _
  private var ControlerFile : String = _
  private var ControlerInHBase : String = _
  private var ShortPath : String = _
  private var INFO : String = _
  private var Split : String = _
  private var OldCarListInHBase : String = _
  private var BlackListOnHBase : String = _
  private var BlackListOfInfoOnHBase : String = _
  private var BlackListFile : String = _
  def this(path : String){
    this()
    val f: File = new File(path)
    p = new Properties()
    var in : BufferedInputStream = null
    try {
      val fis: FileInputStream = new FileInputStream(path)
      in = new BufferedInputStream(fis)
      p.load(in)
    }
    catch {
      case e: IOException => {
        System.out.println("load HBaseInfo.properties error")
        e.printStackTrace()
      }
    }
    GuangZhouMapInHBase = getProperties(p, "GuangZhouMapInHBase")
    GuangZhouMapLongitude = getProperties(p, "GuangZhouMapLongitude")
    HBaseConfPath = getProperties(p, "HBaseConfPath")
    CrossingNumber = getProperties(p, "CrossingNumber")
    TransitTime = getProperties(p, "TransitTime")
    GuangZhouMapLatitude = getProperties(p, "GuangZhouMapLatitude")
    GuangZhouMapGeographicLocation = getProperties(p, "GuangZhouMapGeographicLocation")
    HBaseShortPaths = getProperties(p, "HBaseShortPaths")
    EdgeFileExample = getProperties(p, "EdgeFileExample")
    ControlerFile = getProperties(p, "ControlerFile")
    ControlerInHBase = getProperties(p, "ControlerInHBase")
    INFO = getProperties(p, "INFO")
    ShortPath = getProperties(p, "ShortPath")
    Split = getProperties(p, "Split")
    OldCarListInHBase = getProperties(p, "OldCarListInHBase")
    BlackListOnHBase = getProperties(p, "BlackListOnHBase")
    BlackListOfInfoOnHBase = getProperties(p, "BlackListOfInfoOnHBase")
    BlackListFile = getProperties(p, "BlackListFile")
    System.out.println("Load HBaseInfo.properties from " + f.getAbsoluteFile + "\n" +
      "HBaseConfPath:\t" + HBaseConfPath + "\n" +
      "HBaseShortPaths:\t" + HBaseShortPaths + "\n" +
      "TransitTime:\t" + TransitTime + "\n" +
      "CrossingNumber:\t" + CrossingNumber + "\n" +
      "GuangZhouMapInHBase:\t" + GuangZhouMapInHBase + "\n" +
      "GuangZhouMapGeographicLocation:\t" + GuangZhouMapGeographicLocation + "\n" +
      "GuangZhouMapLongitude:\t" + GuangZhouMapLongitude + "\n" +
      "GuangZhouMapLatitude:\t" + GuangZhouMapLatitude + "\n" +
      "EdgeFileExample:\t" + EdgeFileExample + "\n" +
      "ControlerFile:\t" + ControlerFile + "\n" +
      "ControlerInHBase:\t" + ControlerInHBase + "\n" +
      "INFO:\t" + INFO + "\n" +
      "ShortPath:\t" + ShortPath + "\n" +
      "Split:\t" + Split + "\n" +
      "OldCarListInHBase:\t" + OldCarListInHBase + "\n" +
      "BlackListOnHBase:\t" + BlackListOnHBase + "\n" +
      "BlackListOfInfoOnHBase:\t" + BlackListOfInfoOnHBase + "\n" +
      "BlackListFile:\t" + BlackListFile)
  }
  def getProperties(p: Properties, pro: String): String = {
    p.getProperty(pro)
  }
  def getBlackListFile : String = {
    BlackListFile
  }
  def getBlackListOfInfoOnHBase : String = {
    BlackListOfInfoOnHBase
  }
  def getBlackListOnHBase : String = {
    BlackListOnHBase
  }
  def getOldCarListInHBase : String = {
    OldCarListInHBase
  }
  def getSplit : String = {
    Split
  }
  def getShortPath : String = {
    ShortPath
  }
  def getINFO : String = {
    INFO
  }
  def getControlerInHBase : String = {
    ControlerInHBase
  }
  def getControlerFile : String = {
    ControlerFile
  }
  def getEdgeFileExample : String = {
    EdgeFileExample
  }
  def getTransitTime: String = {
    TransitTime
  }
  def getCrossingNumber: String = {
    CrossingNumber
  }
  def getHBaseConfPath: String = {
    HBaseConfPath
  }
  def getHBaseShortPaths: String = {
    HBaseShortPaths
  }
  def getGuangZhouMapGeographicLocation: String = {
    GuangZhouMapGeographicLocation
  }
  def getGuangZhouMapLatitude: String = {
    GuangZhouMapLatitude
  }
  def getGuangZhouMapLongitude: String = {
    GuangZhouMapLongitude
  }
  def getGuangZhouMapInHBase: String = {
    GuangZhouMapInHBase
  }
}

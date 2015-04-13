package com.xy.lr.tics.properties


import java.io.{FileNotFoundException, FileInputStream, BufferedInputStream, File}
import java.util.Properties

/**
 * Created by xylr on 15-4-9.
 */
class TICSInfo {
  private var p : Properties = _
  //SparkSQLServer 的端口号
  private var SparkSQLServerPort : String= _
  //卡口服务器的端口号
  private var DeZhouServerPort : String = _
  //SparkSQLServer 的url地址
  private var SparkSQLServerUrl : String = _
  //卡口服务器地址
  private var DeZhouServerUrl : String = _
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
      case e: FileNotFoundException =>
        System.err.println("TICSSparkSQLInfo.properties file not found!")
      case _ =>
        System.err.println("load TICSSparkSQLInfo.properties error")
    }
    //导入配置文件数据
    this.SparkSQLServerPort = getProperties(p, "SparkSQLServerPort")
    this.DeZhouServerPort = getProperties(p, "DeZhouServerPort")
    this.SparkSQLServerUrl = getProperties(p, "SparkSQLServerUrl")
    this.DeZhouServerUrl = getProperties(p, "DeZhouServerUrl")

    val propertiesFileString = "Load TICSSparkSQLInfo.properties from " + f.getAbsoluteFile
    val ss = "-" * (propertiesFileString.length + 2)

    val sql = "SparkSQLServerPort: " + SparkSQLServerPort
    val dzs = "DeZhouServerPort: " + DeZhouServerPort
    val sssu = "SparkSQLServerUrl: " + SparkSQLServerUrl
    val dzsu = "DeZhouServerUrl: " + DeZhouServerUrl

    val bl = " " * (ss.length - sql.length - 2)
    val dzsbl = " " * (ss.length - dzs.length - 2)
    val sssul = " " * (ss.length - sssu.length - 2)
    val dzsul = " " * (ss.length - dzsu.length - 2)

    //输出导入的配置文件信息
    System.out.println(
      ss + "\n" +
      "|" + propertiesFileString + "|" + "\n" + ss + "\n" +
      "|" + sql + bl + "|" + "\n" +
      "|" + dzs + dzsbl + "|" + "\n" +
      "|" + sssu + sssul + "|" + "\n" +
      "|" + dzsu + dzsul + "|" + "\n" +
      ss)
  }
  def getProperties(p: Properties, pro: String): String = {
    p.getProperty(pro)
  }
  def getDeZhouServerUrl : String = {
    DeZhouServerUrl
  }
  def getSparkSQLServerUrl : String = {
    SparkSQLServerUrl
  }
  def getSparkSQLServerPort : String = {
    SparkSQLServerPort
  }
  def getDeZhouServerPort : String = {
    DeZhouServerPort
  }
}
/*
object TICSInfo{
  def main(args : Array[String]): Unit ={
    val info = new TICSInfo("TICSInfo.properties")
  }
}
*/
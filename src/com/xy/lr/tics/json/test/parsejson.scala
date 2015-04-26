package com.xy.lr.tics.json.test

import com.xy.lr.java.json.JSONObject

/**
 * Created by xylr on 15-4-26.
 */
object parsejson {
  def main(args : Array[String]): Unit ={
    //解析字符串
    val jsonObject1 = new JSONObject()
    jsonObject1.put("Name", "Tom")
    jsonObject1.put("age", "11")
    System.out.println("解析字符串:" + "\r" + jsonObject1.getString("Name"))

    //解析对象
    val jsonObject = new JSONObject("{'Name':'Tom','age':'11'}")
    val jsonObject2 = new JSONObject()
    jsonObject2.put("jsonObject", jsonObject)

    System.out.println("解析对象:")
    System.out.println(jsonObject2.getJSONObject("jsonObject").toString)
  }
}
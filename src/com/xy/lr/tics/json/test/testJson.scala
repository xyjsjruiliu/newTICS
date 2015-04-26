package com.xy.lr.tics.json.test

import com.xy.lr.java.json.{JSONArray, JSONObject}

/**
 * Created by xylr on 15-4-25.
 */
object testJson{
  def main(args : Array[String]) = {
    //初始化JSONObject 方法一
    val jsonObject1 = new JSONObject()
    jsonObject1.put("Name", "Tom")
    jsonObject1.put("age", "11")

    //初始化JSONObject 方法二
    val jsonObject2 = new JSONObject("{'Name':'Tom','age':'11'}")

    //初始化JSONArray 方法一
    val jsonArray1 = new JSONArray()
    jsonArray1.put("abc")
    jsonArray1.put("xyz")

    //初始化JSONArray 方法二
    val jsonArray2 = new JSONArray("['abc','xyz']")

    println("jsonObject1:" + "\t" + jsonObject1.toString)
    println("jsonObject2:" + "\t" + jsonObject2.toString)
    println("jsonArray1:" + "\t" + jsonArray1.toString)
    println("jsonArray2:" + "\t" + jsonArray2.toString)
  }
}

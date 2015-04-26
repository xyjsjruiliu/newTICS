package com.xy.lr.tics.json.test

import com.xy.lr.java.json.{JSONArray, JSONObject}

/**
 * Created by xylr on 15-4-26.
 */
object testjson {
  def main(args : Array[String]): Unit ={
    val jsonObject = new JSONObject("{'Name':'Tom','age':'11'}")
    val jsonArray = new JSONArray("['abc','xyz']")

    //JSONObject内嵌JSONObject
    val jsonObject1 = new JSONObject()
    jsonObject1.put("object1", jsonObject)
    jsonObject1.put("object2", jsonObject)

    //JSONObject内嵌JSONArray
    val jsonObject2 = new JSONObject()
    jsonObject2.put("JSONArray1", jsonArray)
    jsonObject2.put("JSONArray2", jsonArray)

    //JSONArray内嵌JSONArray
    val jsonArray1 = new JSONArray()
    jsonArray1.put(jsonArray)
    jsonArray1.put(jsonArray)

    //JSONArray内嵌JSONObject
    val jsonArray2 = new JSONArray()
    jsonArray2.put(jsonObject)
    jsonArray2.put(jsonObject)

    System.out.println("JSONObject内嵌JSONObject:" + "\n" + jsonObject1.toString)
    System.out.println("JSONObject内嵌JSONArray:" + "\n" + jsonObject2.toString)
    System.out.println("JSONArray内嵌JSONArray:" + "\n" + jsonArray1.toString)
    System.out.println("JSONArray内嵌JSONObject:" + "\n" + jsonArray2.toString)
  }
}

package com.xy.lr.tics.json.test

/**
 * Created by xylr on 15-4-26.
 * 状态码 枚举类型 + 单例类型
 */
object StatusOfJson extends Enumeration{
  //操作成功
  val SUCCESS = Value("E00")

  //参数格式错误
  val INVALID_ARGUMENTS = Value("E01")

  //内部错误
  val INTERNAL_ERROR = Value("E02")

  //已存在
  val ALREADY_EXIST = Value("E03")

  //不存在
  val INEXISTENCE = Value("E04")

  def main(args : Array[String]): Unit ={
    println(StatusOfJson.SUCCESS)
  }
}


package com.xy.lr.tics.json

/**
 * Created by xylr on 15-4-26.
 * 状态码 单例类型
 */
object StatusOfJson {
  //操作成功
  def SUCCESS : String = {
    "E00"
  }

  //参数格式非法
  def INVALID_ARGUMENTS : String = {
    "E01"
  }

  //内部错误
  def INTERNAL_ERROR : String = {
    "E02"
  }

  //已存在
  def ALREADY_EXIST : String = {
    "E03"
  }

  //不存在
  def INEXISTENCE : String = {
    "E04"
  }
}

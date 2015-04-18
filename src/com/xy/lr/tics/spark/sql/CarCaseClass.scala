package com.xy.lr.tics.spark.sql

/**
 * Created by xylr on 15-4-11.
 */
//样板类定义了 车辆信息表的结构(carNumber, crossNumber, time, graph, direction)
case class CarCaseClass(carNumber : String, crossNumber : String,
                        time : String, graph : String, direction : String)
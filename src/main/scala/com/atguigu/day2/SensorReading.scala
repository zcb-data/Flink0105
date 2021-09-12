package com.atguigu.day2

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/12 22:00
 * @desc：id：传感器ID timestamp：时间戳；temperature：温度值
 */
case class SensorReading(id:String,
                         timestamp:Long,
                         temperature:Double)

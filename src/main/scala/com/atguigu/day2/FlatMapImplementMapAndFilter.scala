package com.atguigu.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/14 18:08
 * @desc：
 */
object FlatMapImplementMapAndFilter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    //使用flatmap实现map功能
    stream.flatMap(new FlatMapFunction[SensorReading,String] {
      override def flatMap(value: SensorReading, out: Collector[String]): Unit = {
        out.collect(value.id)
      }
    }).print()

    //使用flatmap实现filter功能
    stream.flatMap(new FlatMapFunction[SensorReading,SensorReading] {
      override def flatMap(value: SensorReading, out: Collector[SensorReading]): Unit = {
        if(value.id.equals("sensor_1")){
          out.collect(value)
        }
      }
    }).print()

    env.execute()
  }
}

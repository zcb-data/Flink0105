package com.atguigu.day2

import org.apache.flink.streaming.api.scala._

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/16 21:14
 * @desc：Union
 */
object UnionExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val BJ: DataStream[SensorReading] = env.addSource(new SensorSource).filter(_.id.equals("sensor_1"))
    val SH: DataStream[SensorReading] = env.addSource(new SensorSource).filter(_.id.equals("sensor_2"))
    val SZ: DataStream[SensorReading] = env.addSource(new SensorSource).filter(_.id.equals("sensor_3"))

    val union: DataStream[SensorReading] = BJ.union(SH, SZ)
    union.print()
    env.execute()
  }
}

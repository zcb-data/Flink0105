package com.atguigu.day2

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/14 17:20
 * @desc：
 */
object MapExample {4

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
//    stream.map(r => r.id).print()

//    stream.map(new MyMapFunction).print()
    stream.map(new MapFunction[SensorReading,String] {
      override def map(value: SensorReading): String = value.id

    }).print()

    env.execute()

  }


  class MyMapFunction extends MapFunction[SensorReading,String]{
    override def map(value: SensorReading): String = value.id
  }

}

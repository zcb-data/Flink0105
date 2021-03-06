package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/17 21:54
 * @desc：窗口函数
 */
object WindowExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    val keyedStream: KeyedStream[SensorReading, String] = stream.keyBy(_.id)

    val windowedStream: WindowedStream[SensorReading, String, TimeWindow] = keyedStream.timeWindow(Time.seconds(10),Time.seconds(5))

    val reducedStream: DataStream[SensorReading] = windowedStream.reduce((r1, r2) => SensorReading(r1.id, 0L, r1.temperature.min(r2.temperature)))

     reducedStream.print()


    env.execute()

  }
}

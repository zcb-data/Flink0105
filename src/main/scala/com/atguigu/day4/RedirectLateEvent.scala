package com.atguigu.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/26 21:04
 * @desc：迟到数据输出到侧输出流
 */
object RedirectLateEvent {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream  = env.socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr: Array[String] = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignAscendingTimestamps(_._2)
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .sideOutputLateData(new OutputTag[(String, Long)]("late"))
      .process(new Count)
    stream.print()
    stream.getSideOutput(new OutputTag[(String, Long)]("late")).print( )
    env.execute()
  }
class Count extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
  out.collect("窗口中共有"+elements.size + "个元素")
  }
}
}

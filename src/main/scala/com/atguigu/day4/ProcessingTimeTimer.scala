package com.atguigu.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/22 20:32
 * @desc：
 */
object ProcessingTimeTimer {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line =>{
        val arr: Array[String] = line.split(" ")
        (arr(0),arr(1).toLong * 1000)})
      .keyBy(_._1)
      .process(new Keyed)

    stream.print()

    env.execute()
  }
  class Keyed extends KeyedProcessFunction[String,(String,Long),String]{
    //每到一条数据，就会调用一次
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      //注册一个定时器:当前机器时间戳加上10S
      ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 10 * 1000L)
    }
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("定时器触发了！" + "定时器执行的时间戳是" + new Timestamp(timestamp) )
    }
  }
}

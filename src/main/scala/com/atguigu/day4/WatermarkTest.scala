package com.atguigu.day4

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/22 17:26
 * @desc：
 */
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream1 = env
      .socketTextStream("localhost", 9999, '\n')
      .map(line =>{
        val arr: Array[String] = line.split(" ")
        (arr(0),arr(1).toLong * 1000)})
      .assignAscendingTimestamps(_._2)

    val stream2 = env
      .socketTextStream("localhost", 9998, '\n')
      .map(line =>{
        val arr: Array[String] = line.split(" ")
        (arr(0),arr(1).toLong * 1000)})
      .assignAscendingTimestamps(_._2)

    stream1
      .union(stream2)
      .keyBy(_._1)
      .process(new Keyed)
      .print()

    env.execute()
  }
  class Keyed extends KeyedProcessFunction[String,(String,Long),String]{
    //每到一条数据，就会调用一次
    override def processElement(i: (String, Long), context: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
    //输出当前的水位线
      out.collect("当前的水位线是" + context.timerService().currentWatermark())
    }
  }

}

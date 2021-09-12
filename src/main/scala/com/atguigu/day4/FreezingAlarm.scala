package com.atguigu.day4

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/25 21:10
 * @desc：
 */
object FreezingAlarm {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .addSource(new SensorSource)
      //没有keyby开窗！
      .process(new FreezingAlarmFunction)

//    stream.print() //打印常规输出
    stream.getSideOutput(new OutputTag[String]("freezing-alarm")).print() //打印侧输出流,名字必须一样

    env.execute()
  }
  //ProcessFunction 处理的是没有keyby的流
  class FreezingAlarmFunction extends ProcessFunction[SensorReading,SensorReading]{
    //定义一个侧输出标签，也就是侧输出流的名字
    lazy val freezingAlarmOut = new OutputTag[String]("freezing-alarm")

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (value.temperature < 32.0){
        //第一个参数是侧输出流，第二个参数是发送的数据
        ctx.output(freezingAlarmOut,s"${value.id}的传感器低温报警")
      }
      //将所有读数发送到常规输出
      out.collect(value)
    }
  }
}

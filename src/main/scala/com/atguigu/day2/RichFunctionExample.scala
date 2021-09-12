package com.atguigu.day2

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/17 20:46
 * @desc：富函数
 */
object RichFunctionExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[String] = env.fromElements("hello world")
    stream.map(new MyRichMap).print()
    env.execute()
  }

  class MyRichMap extends RichMapFunction[String,String]{
    override def open(parameters: Configuration): Unit = {
      println("生命周期开始了")
    }

    override def map(in: String): String = {
      val name = getRuntimeContext.getTaskName
      "任务的名字是" + name
    }

    override def close(): Unit = {
      println("声明周期结束了")
    }
  }

}

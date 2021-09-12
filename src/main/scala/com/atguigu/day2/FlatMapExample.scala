package com.atguigu.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/14 17:57
 * @desc：
 */
object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
  val stream = env.fromElements("white","gray","black")

    //flatmap，针对流中的每一个元素，生成0个，1个或多个数据
    stream.flatMap(new MyFlatMapFunction).print()

    env.execute()
  }
  class MyFlatMapFunction extends FlatMapFunction[String,String]{

    //调用collect方法，将数据发送到下游
    override def flatMap(t: String, out: Collector[String]): Unit ={
      if(t.equals("white")){
        out.collect(t)
      }else if (t.equals("black")){
        out.collect(t)
        out.collect(t)
      }

    }
  }

}

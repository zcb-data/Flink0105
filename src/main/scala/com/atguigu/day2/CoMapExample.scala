package com.atguigu.day2

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/16 21:25
 * @desc：
 */
object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1: DataStream[(String, Int)] = env.fromElements(("zuoyuan", 130), ("baiyuan", 100))
    val stream2: DataStream[(String, Int)] = env.fromElements(("zuoyuan", 35), ("baiyuan", 33))
    stream1.connect(stream2)

    //直接connect两条流没有意义，要把相同Key的流联合在一起进行计算
    val connected: ConnectedStreams[(String, Int), (String, Int)] = stream1.keyBy(_._1).connect(stream2.keyBy(_._1))
    val printed: DataStream[String] = connected.map(new MyConMapFunction)
    printed.print()
    env.execute()

  }
  class MyConMapFunction extends CoMapFunction[(String,Int),(String,Int),String]{

    //map1处理来自第一条流的元素
    override def map1(value: (String, Int)): String = {
      value._1 + "的体重是" + value._2 + "斤"
    }
     
    //map2处理来自第二条流的元素
    override def map2(value: (String, Int)): String = {

      value._1 + "的年龄是" + value._2 + "岁"
    }


    }



}

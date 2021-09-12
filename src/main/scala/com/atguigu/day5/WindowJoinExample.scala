package com.atguigu.day5

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ：ZCB
 * @date ：Created in 2021/9/1 21:01
 * @desc：基于窗口的JOIN JoinFunction
 */
object WindowJoinExample {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val input1 = {
    env.fromElements(("a",1,1000L),
      ("a",2,2000L),
      ("b",1,3000L),
      ("b",2,4000L))
      .assignAscendingTimestamps(_._3)
    }


      val input2 = {
        env.fromElements(
          ("a",10,1000L),
          ("a",20,2000L),
          ("b",10,3000L),
          ("b",20,4000L))
          .assignAscendingTimestamps(_._3)
    }

      input1
        .join(input2)
        .where(_._1)
        .equalTo(_._1)
        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
        .apply(new MyJoin)
        .print()

    env.execute()

  }

  class MyJoin extends JoinFunction[(String,Int,Long),(String,Int,Long),String]{
    override def join(in1: (String, Int, Long), in2: (String, Int, Long)): String = {
      in1 + "======>" + in2
    }
  }

}

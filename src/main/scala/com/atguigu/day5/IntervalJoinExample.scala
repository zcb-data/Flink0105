package com.atguigu.day5

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/30 22:16
 * @desc：intervalJoin 基于时间的Join
 */
object IntervalJoinExample {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //点击流
    val clickStream = env
      .fromElements(
        ("1","click",3600 * 1000L)
      )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)

    //浏览流

    val browseStream = env.fromElements(
      ("1","browse",2000 * 1000L),
      ("1","browse",3100 * 1000L),
      ("1","browse",3200 * 1000L)
    )
      .assignAscendingTimestamps(_._3)
      .keyBy(_._1)

    clickStream
      .intervalJoin(browseStream)
      // 3600s interval join 3000s ~ 3600s
      .between(Time.seconds(-600),Time.seconds(0))
      .process(new MyInertvalJoin)
      .print()

    env.execute()
  }
  class MyInertvalJoin extends ProcessJoinFunction[(String,String,Long),(String,String,Long),String]{
    override def processElement(left: (String, String, Long), right: (String, String, Long), context: ProcessJoinFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
      //left第一条流，right第二条流
      out.collect(left + "====>" + right)
    }
  }

}

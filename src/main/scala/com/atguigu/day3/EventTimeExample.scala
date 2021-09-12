package com.atguigu.day3

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/21 20:02
 * @desc：事件窗口
 */
object EventTimeExample {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //系统默认每隔200ms插入一次水位线
    //设置每隔一分钟插入一次水位线
    env.getConfig.setAutoWatermarkInterval(60000)

    val stream = env.socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr: Array[String] = line.split(" ")
        //时间时间的单位必须是毫秒
        (arr(0), arr(1).toLong * 1000L)
      })
      //分配时间戳和水位线一定要在keyby之前进行
      .assignTimestampsAndWatermarks(
      //设置时间的最大延迟时间是5S
      new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
        //告诉系统，时间戳至元组的第二个字段
        override def extractTimestamp(element: (String, Long)): Long = element._2
    }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new WindowResult)

    stream.print()
    env.execute()
  }
  class WindowResult extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
    out.collect(new Timestamp(context.window.getStart) + " ~ " + new Timestamp(context.window.getEnd) + "窗口中有"+elements.size + "个元素")
    }
  }

}

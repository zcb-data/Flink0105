package com.atguigu.day3

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.Timestamp

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/21 21:59
 * @desc：水位线生成
 */
object GenWatermark {
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
        new MyAssigner
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new WindowResult)

    stream.print()
    env.execute()
  }
  //周期性的插入水位线

  class MyAssigner extends AssignerWithPeriodicWatermarks[(String,Long)]{
//设置最大延迟时间
    val bound:Long = 10 * 1000L
    //系统观察到的元素包含的最大的时间戳
    var maxTs:Long = Long.MinValue + bound

    //定义抽取时间戳的逻辑，每到一个事件就调用一次
    override def extractTimestamp(element: (String, Long), l: Long): Long = {
    maxTs = maxTs.max(element._2)
      element._2 // 将抽取的时间戳返回
    }
    //产生水位线的逻辑
    //默认每隔200ms调用一次
    override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
    }

  }


  class WindowResult extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart) + " ~ " + new Timestamp(context.window.getEnd) + "窗口中有"+elements.size + "个元素")
    }
  }

}

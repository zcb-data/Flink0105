package com.atguigu.day4

import com.atguigu.day4.RedirectLateEvent.Count
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/26 21:37
 * @desc： 使用迟到元素更新窗口计算结果
 */
object UpdateWindowResultWithLateElement {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream  = env.socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr: Array[String] = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      })
      .assignTimestampsAndWatermarks(
        //最大延迟时间设置为5S
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(_._1)
      //窗口的大小是5S
      .timeWindow(Time.seconds(5))
      //窗口闭合以后等待迟到元素的时间也是5S
      .allowedLateness(Time.seconds(5))
      .process(new UpdateWindowResult)
    stream.print()
    env.execute()
  }
class UpdateWindowResult extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
    //当第一次对窗口进行求值时，也就是水位线超过窗口结束时间
    //会第一次调用process函数
    //这是isupdate为默认值false
    //窗口内初始化一个状态变量，使用windowstate，只对当前窗口可见
  val isUpdate = context.windowState.getState(
    new ValueStateDescriptor[Boolean]("update",Types.of[Boolean])
  )
  if(!isUpdate.value()){
    //当水位线超过窗口结束时间时，第一次调用
    out.collect("窗口第一次求值，元素数量共计：" +elements.size + "个！")
    //第一次调用完process以后，将isUpdate复制为true
    isUpdate.update(true)
  }else{
    out.collect("迟到元素来了!更新的元素数量为" + elements.size + "个！")
  }
  }
  }
}

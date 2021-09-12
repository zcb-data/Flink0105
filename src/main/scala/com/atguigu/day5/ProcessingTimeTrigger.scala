package com.atguigu.day5

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/29 22:03
 * @desc：
 */
object ProcessingTimeTrigger {
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env
      .addSource(new SensorSource)
      .filter(t => t.id.equals("sensor_1"))
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      .trigger(new OneSecondIntervalTrigger )
      .process(new WindowCount)

    stream.print()
    env.execute()
  }
  class OneSecondIntervalTrigger extends Trigger[SensorReading,TimeWindow]{
    //每来一条数据都要调用一次
    override def onElement(t: SensorReading, l: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      //默认值为false
      //当第一条事件来的时候，会在后面的代码中将firstseen置为true
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen",Types.of[Boolean])
      )

      //第一条数据来的时候，!firstSeen.value()为true
      //仅对第一条数据注册处定时器
      //这里的定时器指的是onEventTime函数
      if(!firstSeen.value()){
        //如果当前水位线为1234ms，那么t=2000ms
//        println("第一条数据来了!当前水位线是：" + ctx.getCurrentProcessingTime)

        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))
//        println("第一条数据来了以后，注册的定时器的整数秒的时间戳是:" + t)

        ctx.registerProcessingTimeTimer(t) //在第一条数据的时间戳之后的整数秒注册一个定时器
        ctx.registerProcessingTimeTimer(window.getEnd) //在窗口结束事件注册一个定时器
        firstSeen.update(true)
      }

      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {

      if(time ==  window ){
        //在窗口闭合时，触发计算并清空窗口
        TriggerResult.FIRE_AND_PURGE
      }else{

        val t = ctx.getCurrentProcessingTime + (1000 - (ctx.getCurrentProcessingTime % 1000))
        if(t < window.getEnd) {
//          println("注册的定时器的时间戳是：" + t)
          ctx.registerProcessingTimeTimer(t)
        }
        //触发窗口计算
//        println("在" + time + "触发了窗口计算")
        TriggerResult.FIRE
      }


    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: TriggerContext): TriggerResult = {
      //在onelement函数中，我们注册过窗口结束时间的定时器
      TriggerResult.CONTINUE
    }

    override def clear(w: TimeWindow, ctx: TriggerContext): Unit = {
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("first-seen",Types.of[Boolean])
      )
      firstSeen.clear()
    }
  }


  class WindowCount extends ProcessWindowFunction[SensorReading,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[String]): Unit = {
      out.collect("窗口中有" + elements.size + "条数据!窗口结束时间是：" + context.window.getEnd)
    }
  }
}

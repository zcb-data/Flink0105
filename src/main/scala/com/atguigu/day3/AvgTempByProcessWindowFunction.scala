package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala.function._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import sun.management.Sensor

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/18 21:27
 * @desc：全聚合函数
 */
object AvgTempByProcessWindowFunction {

  case class AvgInfo(id:String,avgTemp:Double,windowStart:Long,windowEnd:Long)

  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)

    val stream : DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new AvgTempFunc)
      .print()

    env.execute()

  }
  //相比于增量聚合函数，缺点是要保存窗口所有的元素
  //增量聚合函数只需要保存一个累加器即可
  //优点是，全窗口聚合函数可以访问窗口信息


  class AvgTempFunc extends ProcessWindowFunction[SensorReading,AvgInfo,String,TimeWindow]{

    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[AvgInfo]): Unit = {

      val count = elements.size
      var sum = 0.0


      for(r <- elements){
        sum += r.temperature
      }
      val windowStart = context.window.getStart
      val windowEnd  = context.window.getEnd
      out.collect(AvgInfo(key,sum / count,windowStart,windowEnd))
    }

  }

}

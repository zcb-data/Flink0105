package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/19 22:24
 * @desc：增量函数实现取温度最大值与最小值
 */
object HighAndLowTemp {

  case class MinMaxTemp(id:String,min:Double,max:Double,endTs:Long)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new HigAndLowAgg,new WindowResult)
      .print()

    env.execute()
  }
  class HigAndLowAgg extends AggregateFunction[SensorReading,(String,Double,Double),(String,Double,Double)]{
    //最小温度值的初始值是Double的最大值
    //最大温度的初始值是Double的最小值
    override def createAccumulator(): (String, Double, Double) = ("",Double.MaxValue,Double.MinValue)

    override def add(in: SensorReading, acc: (String, Double, Double)): (String, Double, Double) = {
      (in.id,acc._2.min(in.temperature),acc._3.max(in.temperature))
    }

    override def getResult(acc: (String, Double, Double)): (String, Double, Double) = acc

    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
      (a._1,a._2.min(b._2),a._3.max(b._3))
    }
  }

  class WindowResult extends ProcessWindowFunction[(String, Double, Double),MinMaxTemp,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val minMax = elements.head
      out.collect(MinMaxTemp(key,minMax._2,minMax._3,context.window.getEnd))
    }
  }
}

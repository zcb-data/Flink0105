package com.atguigu.day3

import com.atguigu.day2.{SensorReading, SensorSource}
import com.atguigu.day3.AvgTempByAggregateFunction.AvgTempAgg
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/19 22:07
 * @desc：
 */
object AvgTempByAggAndProcWindow{
  case class AvgInfo(id:String,avgTemp:Double,windowStart:Long,windowEnd:Long)

  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTempAgg,new WindowResult)
      .print()

    env.execute()

  }
  //第一个泛型：流中元素的类型
  //第二个泛型，累加器的类型，元组（传感器ID，来了多少条温度读数，来的温度读数的总和是多少）
  //第三个泛型，增量聚合函数的输出类型,元组（传感器ID，窗口温度平均值）
  class AvgTempAgg extends AggregateFunction[SensorReading,(String,Long,Double),(String,Double)]{
    //创建空累加器
    override def createAccumulator(): (String, Long, Double) = ("",0L,0.0)

    //聚合逻辑
    override def add(value: SensorReading, acc: (String, Long, Double)): (String, Long, Double) ={
      (value.id,acc._2 + 1,acc._3 + value.temperature)
    }

    //窗口闭合时输出的结果是什么
    override def getResult(acc: (String, Long, Double)): (String, Double) = {
      (acc._1,acc._3 / acc._2)
    }

    //两个累加器合并的逻辑是什么
    override def merge(a: (String, Long, Double), b: (String, Long, Double)): (String, Long, Double) = {
      (a._1,a._2+b._2,a._3+b._3)
    }

  }
  //输入的泛型是增量聚合函数的输出泛型
  class WindowResult extends ProcessWindowFunction[(String,Double),AvgInfo,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Double)], out: Collector[AvgInfo]): Unit = {
//  迭代器中只有一个值，就是增量聚合函数发送过来的聚合结果
    out.collect(AvgInfo(key,elements.head._2,context.window.getStart,context.window.getEnd))
    }
  }
}

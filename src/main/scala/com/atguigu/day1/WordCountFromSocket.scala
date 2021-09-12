package com.atguigu.day1

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/10 23:01
 * @desc：
 */
object WordCountFromSocket {
  case class WordWithCount(value: String, i: Int)

  def main(args: Array[String]): Unit = {
    //获取运行时环境，类似于SparkContext
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置分区(又叫并行任务)的数量为1
    env.setParallelism(1)

    //建立数据源

    val stream = env.socketTextStream("localhost", 9999, '\n')

    //写对流的转换处理逻辑
    val transformed = stream
      //使用空格切分输入的字符串
      .flatMap(line => line.split("\\s"))
      //类似MR中的map
      .map(w => WordWithCount(w, 1))
      //使用word字段进行分区，shuffle
      .keyBy(0)
      //开了一个5s的滚动窗口
      .timeWindow(Time.seconds(5))
      //针对count字段进行累加操作
      .sum(1)

    //将计算的结果输出到标准输出
    transformed.print()
    //执行计算逻辑
    env.execute()
  }
}

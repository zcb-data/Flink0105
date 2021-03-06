package com.atguigu.day1

import org.apache.flink.streaming.api.scala._

/**
 * @author ：ZCB
 * @date ：Created in 2021/8/10 23:08
 * @desc：
 */
object WordCountFromBatch {
  case class WordWithCount(word:String,count:Int)
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      "hello world",
      "hello world",
      "hello world"
    )
    val transformed: DataStream[WordWithCount] = stream.flatMap(line => line.split("\\s"))
      .map(w => WordWithCount(w, 1))
      .keyBy(0)
      .sum(1)

    transformed.print()

    env.execute( )

  }
}

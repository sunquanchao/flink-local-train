package com.lp.java.demo.datastream.mackchao.scala
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
/**
  * @description:
  * @author Mackchao.Sun
  * @date:2022/2/15
  * */
object wordCount11 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDS = env.socketTextStream("bigdata101", 8888)

    dataDS.flatMap(_.split(" "))
      .filter(!_.isEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .process(
        new KeyedProcessFunction[String, (String, Int), String] {
          //定义状态值
          private var wordCount: ValueState[Int] = _

          //这里的open方法，只会在项目启动的时候调用一次
          override def open(parameters: Configuration): Unit = {
            //通过getRuntimeContext.getState 获取State,作为初始值
            wordCount = getRuntimeContext.getState(
              new ValueStateDescriptor[Int]("wordCount", classOf[Int])
            )
          }

          override def processElement(value: (String, Int),
                                      ctx: KeyedProcessFunction[String, (String, Int), String]#Context,
                                      out: Collector[String]): Unit = {
            //更新wordCount的值
            wordCount.update(wordCount.value() + 1)
            out.collect(ctx.getCurrentKey + "的单词数为" + wordCount.value())
          }
        }
      )
      .print()
    env.execute()
  }
}
package com.lp.java.demo.datastream.mackchao;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

/**
 * @author Mackchao.Sun
 * @description:
 * @date:2022/7/25
 **/
public class ReinterpretAsKeyedStreamDemo {

 public void reinterpretAsKeyedStream() throws Exception {
  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  env.setParallelism(1);
  DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 1, 2, 3, 1, 2, 3);

  KeyedStream<Integer, Integer> reinterpret = DataStreamUtils.reinterpretAsKeyedStream(source, new KeySelector<Integer, Integer>() {
   @Override
   public Integer getKey(Integer value) throws Exception {
    return value;
   }
  });

  SingleOutputStreamOperator<Integer> reducer = reinterpret.countWindow(2)
          .reduce(new ReduceFunction<Integer>() {
           @Override
           public Integer reduce(Integer value1, Integer value2) throws Exception {
            return value1 + value2;
           }
          });

  reducer.addSink(new PrintSinkFunction<>());

  env.execute("xx");

 }

}
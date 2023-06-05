package com.mackchao.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.EnvironmentSettings;

/**
 * @author Mackchao.Sun
 * @description:
 * @date:2022/10/28
 **/
public class RetractStreamDemo {
 public static void main(String[] args) throws Exception {
  // set up execution environment
  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
  env.setParallelism(1);
  // use blink planner in streaming mode
  EnvironmentSettings settings = EnvironmentSettings.newInstance()
          .inStreamingMode()
          .build();
  StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
  // 用fromElements模拟非回撤消息
  DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(new Tuple2<>("hello", 1), new Tuple2<>("hello", 1), new Tuple2<>("hello", 1));
  tEnv.registerDataStream("tmpTable", dataStream, "word, num");
  Table table = tEnv.sqlQuery("select cnt, count(word) as freq from (select word, count(num) as cnt from tmpTable group by word) group by cnt");
  // 启用回撤流机制
  tEnv.toRetractStream(table, TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
  })).print();
  env.execute();
 }
}

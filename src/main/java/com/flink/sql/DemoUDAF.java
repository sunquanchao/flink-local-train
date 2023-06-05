package com.flink.sql;

/**
 * @author Mackchao.Sun
 * @description:
 * @date:2022/10/18
 **/
import org.apache.flink.table.functions.AggregateFunction;

/**
 -   功能：求和
 -   注册函数：create function DemoUDAF as 'packagename.DemoUDAF';
 -   使用案例：
 select student_id,DemoUDAF(score)
 from tablename
 group by student_id
 */
// AggregateFunction<聚合的最终结果类型,聚合期间的中间结果类型>
public class DemoUDAF extends AggregateFunction<Long, DemoUDAF.SumAccumulator> {

 //定义一个累加器，存放聚合的中间结果
 public static class SumAccumulator{
  public long sumPrice;
 }

 //初始化累加器
 @Override
 public SumAccumulator createAccumulator() {
  SumAccumulator sumAccumulator = new SumAccumulator();
  sumAccumulator.sumPrice=0;
  return sumAccumulator;
 }

 //根据输入，更新累加器
 //@Override
 public void accumulate(SumAccumulator accumulator,Long input){
  accumulator.sumPrice += input;
 }

 public void accumulates(SumAccumulator accumulator,Long input){
  accumulator.sumPrice += input;
 }
 //返回聚合的最终结果
 @Override
 public Long getValue(SumAccumulator accumulator) {
  return accumulator.sumPrice;
 }
}

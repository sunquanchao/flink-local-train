package com.flink.sql;

/**
 * @author Mackchao.Sun
 * @description:
 * @date:2022/10/18
 **/
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;


/**
 *   功能：根据指定的分隔符，将一行变成多行
 *   注册函数：create function DemoRowMore as 'packagename.DemoRowMore';
 *   使用案例：
 select *,word
 from tablename,LATERAL TABLE(DemoRowMore(fieldname,',')) as T(word)
 */

public class DemoRowMore extends TableFunction<Row> {

 public DemoRowMore() {
 }

 // 使用注解指定输出数据的名称和类型
 // 没有返回值，使用collect收集数据，可以收集多次
 @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
 public void eval(String data,String split){
  String[] arr = data.split(split);
  for (String s : arr) {
   collect(Row.of(s));
  }
 }
}

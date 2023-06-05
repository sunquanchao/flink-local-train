package com.flink.sql;

/**
 * @author Mackchao.Sun
 * @description:
 * @date:2022/10/18
 **/

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 功能：字符串转小写
 * 注册函数：create function DemoUDF as 'packagename.DemoUDF';
 * 使用案例：select DemoUDF('Abc')
 */
public class DemoUDF extends ScalarFunction {

    public DemoUDF() {
    }

    public String eval(String str) {
        return str.toLowerCase();
    }
}
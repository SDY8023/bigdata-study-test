package com.atguigu.tableApi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @ClassName TableTest4
 * @Description
 * @Author SDY
 * @Date 2022/3/20 18:17
 **/
public class TableTest4 {
    public static void main(String[] args) {
        /**
         * 创建执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 因为StreamTableEnvironment就是继承的TableEnvironment,所以可以使用TableEnvironment
        TableEnvironment tableEnv = StreamTableEnvironment.create(env);

    }
}

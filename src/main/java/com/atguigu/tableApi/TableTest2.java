package com.atguigu.tableApi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @Author:SDY
 * @Description:
 * @Date: 21:40 2022/2/24
 * @Modified By:
 **/
public class TableTest2 {
    public static void main(String[] args) {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        // 基于老版本Planner的流处理
        EnvironmentSettings blinkOldStream = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment streamTableEnvironment1 = StreamTableEnvironment.create(env, blinkOldStream);

        // 基于老版本Planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);

        // 基于blink的流处理
        EnvironmentSettings blinkStream = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment streamTableEnvironment2 = StreamTableEnvironment.create(env, blinkStream);

        // 基于blink的批处理
        EnvironmentSettings blinkBatch = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(blinkBatch);

    }
}

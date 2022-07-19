package com.atguigu.tableApi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @Author:SDY
 * @Description:
 * @Date: 21:40 2022/2/24
 * @Modified By:
 **/
public class TableTest2 {
    public static void main(String[] args) throws Exception {
        // 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 基于老版本Planner的流处理
//        EnvironmentSettings blinkOldStream = EnvironmentSettings.newInstance()
//                .useOldPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment streamTableEnvironment1 = StreamTableEnvironment.create(env, blinkOldStream);
//
//        // 基于老版本Planner的批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);
//
//        // 基于blink的流处理
//        EnvironmentSettings blinkStream = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        StreamTableEnvironment streamTableEnvironment2 = StreamTableEnvironment.create(env, blinkStream);

        // 基于blink的批处理
//        EnvironmentSettings blinkBatch = EnvironmentSettings.newInstance()
////                .inStreamingMode()
//                .useBlinkPlanner()
//                .inBatchMode()
//                .build();
//        TableEnvironment tableEnvironment = TableEnvironment.create(blinkBatch);

        // 表的创建：连接外部系统，读取数据
        // 读取文件
        String filePath = "D:\\study\\code\\bigdata-study-test\\src\\main\\resources\\sensor.txt";
        // 连接表设置
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                ).createTemporaryTable("inputTable");
        Table inputTable = tableEnv.from("inputTable");
        inputTable.printSchema();
        tableEnv.toAppendStream(inputTable, Row.class).print();

        Table table = tableEnv.sqlQuery("select * from inputTable");

        env.execute("aa");

    }
}

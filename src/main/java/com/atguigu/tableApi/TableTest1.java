package com.atguigu.tableApi;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @Author:SDY
 * @Description:
 * @Date: 21:51 2022/2/23
 * @Modified By:
 **/
public class TableTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.readTextFile("D:\\IDEA\\DaiMa\\bigdata-study-test\\src\\main\\resources\\sensor.txt");

        SingleOutputStreamOperator<SenorReading> dataSource = dataStream.map(line -> {
            String[] datas = line.split(",");
            return new SenorReading(datas[0], new Long(datas[1]), new Double(datas[2]));
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table dataTable = tableEnv.fromDataStream(dataSource);
        Table id = dataTable.select("id");

        DataStream<String> stringDataStream = tableEnv.toAppendStream(id, String.class);
        stringDataStream.print();
        env.execute();

    }
}

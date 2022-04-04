package com.atguigu.tableApi;

import com.atguigu.beans.SenorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
/**
 * @ClassName TableTest4
 * @Description
 * @Author SDY
 * @Date 2022/3/20 18:17
 **/
public class TableTest4 {
    public static void main(String[] args) throws Exception {
        /**
         * 创建执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 读入文件数据，得到dataStream
        DataStreamSource<String> data = env.readTextFile("D:\\study\\code\\bigdata-study-test\\src\\main\\resources\\sensor.txt");

        SingleOutputStreamOperator<SenorReading> data2 = data.map(line -> {
            String[] split = line.split(",");
            return new SenorReading(split[0], new Long(split[1]), new Double(split[2]));
        });

        // 将流转换成表，定义时间特性
        Table table = tableEnv.fromDataStream(data2, "id,timestamp,temperature,pt.proctime");
        table.printSchema();
        tableEnv.toAppendStream(table,Row.class).print();

        env.execute();


    }
}

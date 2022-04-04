package com.atguigu.tableApi;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName TableTest3
 * @Description
 * @Author SDY
 * @Date 2022/3/13 11:58
 **/
public class TableTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        inputTable.createTemporalTableFunction("inputTable","1");


        // 表的创建：连接外部系统，读取数据
        // 读取文件
//        String filePath = "D:\\study\\code\\bigdata-study-test\\src\\main\\resources\\sensor.txt";
//        // 连接表设置
//        tableEnv.connect(new FileSystem().path(filePath))
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                        .field("id", DataTypes.STRING())
//                        .field("ts", DataTypes.BIGINT())
//                        .field("temp", DataTypes.DOUBLE())
//                ).createTemporaryTable("inputTable");
//        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

//        Table table = tableEnv.sqlQuery("select * from inputTable");

        // 接受指定端口的数据
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("192.168.248.101", 7777);

        SingleOutputStreamOperator<SenorReading> map = stringDataStreamSource.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SenorReading(split[0], Long.valueOf(split[1]), Double.valueOf(split[2]));
            }
        });

        Table inputTable = tableEnv.fromDataStream(map);

        Table table = inputTable.select("id,timestamp").filter("id = 'sensor_1'");

        Table id = inputTable.groupBy("id")
                .select("id,temperature.sum as total_temp");
        // toAppendStream是向表中追加数据，不可以更新数据
        tableEnv.toAppendStream(table,Row.class).print();
        System.out.println("--------------");
        // toRetractStream是更新数据，所以可以使用聚合函数
        tableEnv.toRetractStream(id, Row.class).print();

        env.execute("aa");

    }
}

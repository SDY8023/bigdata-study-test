package com.atguigu.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName TransformTest1
 * @Description
 * @Author SDY
 * @Date 2021/12/11 14:56
 **/
public class TransformTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataSource = env.readTextFile("D:\\study\\code\\flink\\FlinkTest\\src\\main\\resources\\sensor.txt");

        // 1 map操作 SingleOutputStreamOperator 继承的是DataStream 因此此处可以直接换
        DataStream<Integer> mapDataStream = dataSource.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return value.length();
            }
        });

        // 2 flatMap
        SingleOutputStreamOperator<String> flatMapDataStream = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String element : value.split(",")) {
                    out.collect(element);
                }
            }
        });

        // 2 filter
        SingleOutputStreamOperator<String> filterDataStream = dataSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });

        // 打印输出
        mapDataStream.print("map");
        flatMapDataStream.print("flatMap");
        filterDataStream.print("filter");

        // 执行
        env.execute();

    }
}

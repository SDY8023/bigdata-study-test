package com.atguigu.transform;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @ClassName TransformTest2
 * @Description
 * @Author SDY
 * @Date 2021/12/14 21:05
 **/
public class TransformTest4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("D:\\study\\code\\flink\\FlinkTest\\src\\main\\resources\\sensor.txt");

       // 普通的写法
        DataStream<SenorReading> mapSource = source.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] data = value.split(",");
                return new SenorReading(data[0], new Long(data[1]), new Double(data[2]));
            }
        });

        // 1 分流，按照温度值
        SplitStream<SenorReading> splitStream = mapSource.split(new OutputSelector<SenorReading>() {
            @Override
            public Iterable<String> select(SenorReading value) {
                return value.getTemperature() > 35.5 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SenorReading> high = splitStream.select("high");
        DataStream<SenorReading> low = splitStream.select("low");

        // 2 合流 connect
        SingleOutputStreamOperator<Tuple2<String, Double>> warringStream = high.map(new MapFunction<SenorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SenorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SenorReading> connectStream = warringStream.connect(low);
        SingleOutputStreamOperator<Object> resultStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SenorReading, Object>() {
            // highStream
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temperature warring!");
            }

            // lowStream
            @Override
            public Object map2(SenorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        resultStream.print();

        // 3 union  联合多条流 流的数据类型必须一致
        high.union(low).print("union");




        env.execute();

    }
}

package com.atguigu.window;

import com.atguigu.beans.SenorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName WindowTest1
 * @Description 窗口函数：增量窗口，全量窗口
 * @Author SDY
 * @Date 2021/12/25 13:42
 **/
public class WindowTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataStreamSource<String> source = env.readTextFile("D:\\study\\code\\flink\\FlinkTest\\src\\main\\resources\\sensor.txt");
        DataStreamSource<String> source = env.socketTextStream("192.168.248.101",7777);
//        env.setParallelism(4);

        // 普通的写法
        DataStream<SenorReading> mapSource = source.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] data = value.split(",");
                return new SenorReading(data[0], new Long(data[1]), new Double(data[2]));
            }
        });

        // 计数窗口:计算输入的数据的平均温度值
        SingleOutputStreamOperator<Double> result = mapSource.keyBy("id")
                .countWindow(10, 2)
                .aggregate(new AggregateFunction<SenorReading, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        // 创建累加器，并赋予初始值
                        return new Tuple2<>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SenorReading value, Tuple2<Double, Integer> accumulator) {
                        // 对每次进来的数据做处理
                        return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                });
        result.print();
        env.execute();
    }
}

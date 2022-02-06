package com.atguigu.window;

import com.atguigu.beans.SenorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;

/**
 * @ClassName WindowTest1
 * @Description 窗口函数：增量窗口，全量窗口
 * @Author SDY
 * @Date 2021/12/25 13:42
 **/
public class WindowTest1 {
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

        // 增量窗口函数: 对传入数值进行计数
        SingleOutputStreamOperator<Integer> result = mapSource.keyBy("id")
                //.timeWindow(Time.seconds(5))  时间窗口适合长进程的数据源
                .timeWindow(Time.seconds(15))
                //.countWindow(3)
                .aggregate(new AggregateFunction<SenorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        // 此方法实现一个累加器：附一个累加初始值
                        return 0;
                    }

                    @Override
                    public Integer add(SenorReading value, Integer accumulator) {
                        // 累加
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a+b;
                    }
                });

        // 全量窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> result2 = mapSource.keyBy("id")
                .timeWindow(Time.seconds(15))
                .apply(new WindowFunction<SenorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SenorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {

                        String id = tuple.getField(0);
                        long end = window.getEnd();
                        Integer size = IteratorUtils.toList(input.iterator()).size();

                        out.collect(new Tuple3<>(id, end, size));

                    }

                });

        result2.print();

        env.execute();
    }
}

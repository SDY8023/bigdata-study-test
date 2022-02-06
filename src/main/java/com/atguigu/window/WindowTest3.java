package com.atguigu.window;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName WindowTest3
 * @Description
 * @Author SDY
 * @Date 2022/1/9 21:05
 **/
public class WindowTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(3);

        DataStreamSource<String> source = env.socketTextStream("192.168.248.101", 7777);

        DataStream<SenorReading> mapSource = source.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] data = value.split(",");
                return new SenorReading(data[0], new Long(data[1]), new Double(data[2]));
            }
        })      // 乱序数据设置时间戳watermark
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SenorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SenorReading element) {
                return element.getTimestamp() * 1000;
            }
        });

        OutputTag<SenorReading> late = new OutputTag<SenorReading>("late"){

        };
        // 基于事件时间的开窗，统计15秒内温度的最小值
        SingleOutputStreamOperator<SenorReading> result = mapSource.keyBy("id")
                .timeWindow(Time.seconds(15))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(late)
                .minBy("temperature");
        result.print("minTemp");
        result.getSideOutput(late).print("late");

        env.execute();


    }
}

package com.atguigu.transform;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName TransformTest2
 * @Description
 * @Author SDY
 * @Date 2021/12/14 21:05
 **/
public class TransformTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("D:\\study\\code\\flink\\FlinkTest\\src\\main\\resources\\sensor.txt");

        env.setParallelism(1);
       // 普通的写法
        DataStream<SenorReading> mapSource = source.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] data = value.split(",");
                return new SenorReading(data[0], new Long(data[1]), new Double(data[2]));
            }
        });
        KeyedStream<SenorReading, Tuple> keyedStream = mapSource.keyBy("id");

        // reduce算子，参数1：当前状态，参数2：新进来的数据
        SingleOutputStreamOperator<SenorReading> resultStream = keyedStream.reduce(new ReduceFunction<SenorReading>() {
            @Override
            public SenorReading reduce(SenorReading value1, SenorReading value2) throws Exception {
                return new SenorReading(value1.getId(), value2.getTimestamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        resultStream.print();


        env.execute();


    }
}

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
public class TransformTest5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("D:\\study\\code\\flink\\FlinkTest\\src\\main\\resources\\sensor.txt");
        env.setParallelism(4);

       // 普通的写法
        DataStream<SenorReading> mapSource = source.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] data = value.split(",");
                return new SenorReading(data[0], new Long(data[1]), new Double(data[2]));
            }
        });
        source.print("input");
////        mapSource.keyBy("id").print("keyBy").setParallelism(5);
//
        source.shuffle().print("shuffle");
////
        source.global().print("global");





        env.execute();


    }
}

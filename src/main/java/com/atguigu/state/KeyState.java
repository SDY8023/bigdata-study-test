package com.atguigu.state;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName KeyState
 * @Description
 * @Author SDY
 * @Date 2022/1/12 20:47
 **/
public class KeyState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\study\\code\\flink\\FlinkTest\\src\\main\\resources\\sensor.txt");

        DataStream<SenorReading> mapSource = source.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] data = value.split(",");
                return new SenorReading(data[0], new Long(data[1]), new Double(data[2]));
            }
        });

        SingleOutputStreamOperator<Integer> id = mapSource.keyBy("id").map(new MyMapperFunction());

        id.print();

        env.execute();
    }
    public static class MyMapperFunction extends RichMapFunction<SenorReading,Integer>{

        // 声明状态
        private ValueState<Integer> keyCountState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 从运行时上下文中获取状态
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class,0));
        }

        @Override
        public Integer map(SenorReading value) throws Exception {

            Integer count = keyCountState.value();


            // 进行逻辑操作
            count++;

            // 更新状态
            keyCountState.update(count);
            return count;
        }
    }
}

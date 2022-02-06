package com.atguigu.state;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.crypto.Mac;

/**
 * @ClassName StateTest
 * @Description
 * @Author SDY
 * @Date 2022/1/12 21:31
 **/

public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("192.168.248.101", 7777);

        DataStream<SenorReading> mapSource = source.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] data = value.split(",");
                return new SenorReading(data[0], new Long(data[1]), new Double(data[2]));
            }
        });

        SingleOutputStreamOperator<Tuple3<String,Double,Double>> id = mapSource.keyBy("id")
                .flatMap(new MyFlatMapFuntion(5.0));

        id.print();
        env.execute();

    }
    public static class MyFlatMapFuntion extends RichFlatMapFunction<SenorReading, Tuple3<String,Double,Double>>{
        // 定义默认温度阈值
       private Double threshold;

       public MyFlatMapFuntion(Double threshold) {
           this.threshold = threshold;
       }

       // 定义状态:只存上次温度值
        private ValueState<Double> lastTempState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp",Double.class));
        }

        @Override
        public void flatMap(SenorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {

            // 获取状态中存储的上次的温度值
            Double lastTemp = lastTempState.value();

            if(lastTemp != null){
                double abs = Math.abs(value.getTemperature() - lastTemp);
                if(abs > threshold){
                    out.collect(new Tuple3<>(value.getId(),lastTemp,value.getTemperature()));
                }
            }

            // 更新状态值
            lastTempState.update(value.getTemperature());

        }

        // 当关闭方法时，清空状态
        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }



    }
}

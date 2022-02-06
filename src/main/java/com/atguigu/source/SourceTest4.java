package com.atguigu.source;

import com.atguigu.beans.SenorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @ClassName SourceTest4ZiDingYI
 * @Description 自定义source
 * @Author SDY
 * @Date 2021/12/8 22:16
 **/
public class SourceTest4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据源
        DataStream<SenorReading> data = env.addSource(new MySensorSource());

        data.print();
        env.execute();
    }

    /**
     * 实现自定义的SourceFunction
     */
    public static class MySensorSource implements SourceFunction<SenorReading>{

        // 定义一个标识位，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SenorReading> ctx) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();

            // 设置 10个传感器的初始温度
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for(int i = 0; i < 10; i++){
                sensorTempMap.put("sensor_" + (i+1),60 + random.nextGaussian() * 20);
            }

            while (running){
                for(String id : sensorTempMap.keySet()){
                    // 在当前的温度基础上随机波动
                    Double newTemp = sensorTempMap.get(id) + random.nextGaussian();
                    sensorTempMap.put(id,newTemp);
                    ctx.collect(new SenorReading(id,System.currentTimeMillis(),newTemp));
                }
                // 控制输出
                Thread.sleep(1000L);
                cancel();
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

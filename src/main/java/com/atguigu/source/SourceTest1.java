package com.atguigu.source;

import com.atguigu.beans.SenorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @ClassName SourceTest1
 * @Description Flink数据源生成
 * @Author SDY
 * @Date 2021/12/8 21:43
 **/
public class SourceTest1 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合中读取数据
        DataStream<SenorReading> dataStream1 = env.fromCollection(Arrays.asList(
                new SenorReading("sensor_1", 1547718201L, 35.2),
                new SenorReading("sensor_1", 1547718202L, 35.5),
                new SenorReading("sensor_1", 1547718203L, 35.6),
                new SenorReading("sensor_1", 1547718204L, 32.1),
                new SenorReading("sensor_1", 1547718205L, 23.5)
        ));

        DataStreamSource<Integer> dataStream2 = env.fromElements(1, 2, 3, 4, 54, 65, 7, 8, 4, 24, 564, 5643, 42, 23, 456);

        // 打印输出
        dataStream1.print("data1");
        dataStream2.print("data2");

        // 执行
        env.execute("SourceTest");
    }
}

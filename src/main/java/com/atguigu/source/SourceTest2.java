package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName SourceTest2
 * @Description
 * @Author SDY
 * @Date 2021/12/8 21:56
 **/
public class SourceTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        // 从文件读取数据
        DataStreamSource<String> dataStream = env.readTextFile("D:\\study\\code\\flink\\FlinkTest\\src\\main\\resources\\sensor.txt");
        dataStream.print();
        env.execute();
    }
}

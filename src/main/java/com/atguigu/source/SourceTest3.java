package com.atguigu.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

/**
 * @ClassName SourceTest3
 * @Description
 * @Author SDY
 * @Date 2021/12/8 22:02
 **/
public class SourceTest3 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        // 设置好kafka的连接配置参数即可 properties.setProperty()

        // 创建数据源
        // env.addSource(new FlinkKafkaConsumer011<Object>("sensor",new Si));


    }
}

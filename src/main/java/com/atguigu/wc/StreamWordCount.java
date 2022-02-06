package com.atguigu.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception{
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定文件
//        String path = "D:\\study\\code\\flink\\FlinkTest\\src\\main\\java\\hello.txt";

        // 用parameter tool 工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // DataStream<String> inputDataSource = env.readTextFile(path);
        DataStream<String> inputDataSource = env.socketTextStream(host,port);

        // 进行数据处理
        DataStream<Tuple2<String, Integer>> result = inputDataSource.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);

        result.print();

        // 因为这里是流处理，因此，上边的代码只是将流数据的处理逻辑写好了，但是触发任务的开始，是需要单独调起来的
        env.execute();


    }
}

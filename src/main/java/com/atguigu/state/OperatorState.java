package com.atguigu.state;

import com.atguigu.beans.SenorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName OperatorState
 * @Description 算子状态
 * @Author SDY
 * @Date 2022/1/11 21:45
 **/
public class OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("D:\\\\study\\\\code\\\\flink\\\\FlinkTest\\\\src\\\\main\\\\resources\\\\sensor.txt");

        DataStream<SenorReading> mapSource = source.map(new MapFunction<String, SenorReading>() {
            @Override
            public SenorReading map(String value) throws Exception {
                String[] data = value.split(",");
                return new SenorReading(data[0], new Long(data[1]), new Double(data[2]));
            }
        });

        SingleOutputStreamOperator<Integer> map = mapSource.map(new Mapper());

        map.print();
        env.execute();
    }
    public static class Mapper implements MapFunction<SenorReading,Integer>, ListCheckpointed<Integer>{

        private int count = 0;
        @Override
        public Integer map(SenorReading value) throws Exception {
            return count++;
        }

        // 保存状态list
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(count);
        }

        // 恢复状态
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for(Integer num : state){
                count ++;
            }
        }
    }

}
